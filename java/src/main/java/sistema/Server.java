package sistema;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import sistema.proto.ConnReply;
import sistema.proto.CreateChannelReply;
import sistema.proto.ErrorReply;
import sistema.proto.ListChannelsReply;
import sistema.proto.LoginReply;
import sistema.proto.Publication;
import sistema.proto.PublishReply;
import sistema.proto.PublishRequest;
import sistema.proto.Request;
import sistema.proto.Response;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Set;

/** Handles Req-Rep requests, Pub-Sub publications, and server-side persistence. */
final class Server {
    private static final Set<String> USERS = Set.of("pedro_client", "gabriel_client");

    private Server() {}

    static void run() throws Exception {
        try (Connection database = Database.connect(); ZContext context = new ZContext()) {
            Database.initializeSchema(database);

            String serverName = System.getenv().getOrDefault("SERVER_NAME", "server");
            try (ReferenceClient reference = new ReferenceClient(context)) {
                int rank = reference.register(serverName);
                System.out.printf("servidor '%s' registrado com rank %d%n", serverName, rank);
                for (var server : reference.listServers()) {
                    System.out.printf("servidor disponível: %s (rank %d)%n",
                            server.getName(), server.getRank());
                }

                ZMQ.Socket replies = context.createSocket(ZMQ.REP);
                replies.connect("tcp://broker:5556");
                ZMQ.Socket publications = context.createSocket(ZMQ.PUB);
                publications.connect("tcp://proxy:5557");
                int receivedRequests = 0;

                while (!Thread.currentThread().isInterrupted()) {
                    byte[] rawRequest = replies.recv(0);
                    Request request = Request.parseFrom(rawRequest);
                    TimeUtil.observeLogicalClock(request.getClock());
                    var requestTimestamp = request.hasTimestamp() ? request.getTimestamp() : TimeUtil.now();
                    System.out.printf("[%s] recebido: %s | relógio lógico: %d%n",
                            TimeUtil.format(requestTimestamp), request.getPayloadCase(),
                            TimeUtil.currentLogicalClock());

                    receivedRequests++;
                    boolean heartbeatDue = receivedRequests % 10 == 0;

                    Database.saveMessage(database, "in", request.getPayloadCase()
                            .name().toLowerCase(Locale.ROOT), rawRequest, requestTimestamp,
                            request.getClock());

                    Response response = handle(database, publications, request).toBuilder()
                            .setTimestamp(TimeUtil.now())
                            .setClock(TimeUtil.incrementLogicalClock())
                            .build();
                    Database.saveMessage(database, "out", response.getPayloadCase()
                            .name().toLowerCase(Locale.ROOT), response.toByteArray(),
                            response.getTimestamp(), response.getClock());
                    replies.send(response.toByteArray(), 0);

                    if (heartbeatDue) {
                        int heartbeatStatus = reference.heartbeat(serverName);
                        if (heartbeatStatus == 404) {
                            int newRank = reference.register(serverName);
                            System.out.printf("registrado novamente com rank %d%n", newRank);
                        } else if (heartbeatStatus == 200) {
                            System.out.printf("heartbeat ok | ajuste do relógio: %+.3f ms%n",
                                    TimeUtil.physicalOffsetMillis());
                        } else {
                            System.out.println("heartbeat sem resposta da referência");
                        }
                    }
                }
            }
        }
    }

    private static Response handle(Connection database, ZMQ.Socket publications, Request request)
            throws SQLException {
        Response.Builder response = Response.newBuilder();

        switch (request.getPayloadCase()) {
            case CONN -> response.setConnReply(ConnReply.newBuilder()
                    .setMessage("pong: " + request.getConn().getMessage())
                    .setTimestamp(TimeUtil.now()));
            case LOGIN -> handleLogin(database, request, response);
            case CREATE_CHANNEL -> handleCreateChannel(database, request, response);
            case LIST_CHANNELS -> handleListChannels(database, response);
            case PUBLISH -> handlePublish(database, publications, request.getPublish(), response);
            case PAYLOAD_NOT_SET -> response.setError(ErrorReply.newBuilder()
                    .setError("unknown command: empty")
                    .setTimestamp(TimeUtil.now()));
        }

        return response.build();
    }

    private static void handleLogin(Connection database, Request request, Response.Builder response)
            throws SQLException {
        String username = request.getLogin().getUser();
        System.out.println("Tentativa de login: " + username);
        int status = USERS.contains(username) ? 200 : 401;

        if (status == 200) {
            String sql = "INSERT INTO login_events(username,logged_at) VALUES(?,?)";
            try (PreparedStatement statement = database.prepareStatement(sql)) {
                statement.setString(1, username);
                statement.setTimestamp(2, java.sql.Timestamp.from(java.time.Instant.now()));
                statement.executeUpdate();
            }
        }

        response.setLoginReply(LoginReply.newBuilder()
                .setStatus(status)
                .setTimestamp(TimeUtil.now()));
    }

    private static void handleCreateChannel(Connection database, Request request,
                                            Response.Builder response) throws SQLException {
        String sql = "INSERT INTO channels(name) VALUES(?) ON CONFLICT DO NOTHING";
        try (PreparedStatement statement = database.prepareStatement(sql)) {
            statement.setString(1, request.getCreateChannel().getName());
            int status = statement.executeUpdate() == 1 ? 200 : 409;
            response.setCreateChannelReply(CreateChannelReply.newBuilder()
                    .setStatus(status)
                    .setTimestamp(TimeUtil.now()));
        }
    }

    private static void handleListChannels(Connection database, Response.Builder response)
            throws SQLException {
        ListChannelsReply.Builder reply = ListChannelsReply.newBuilder()
                .setTimestamp(TimeUtil.now());
        try (Statement statement = database.createStatement();
             ResultSet rows = statement.executeQuery("SELECT name FROM channels ORDER BY name")) {
            while (rows.next()) {
                reply.addChannels(rows.getString(1));
            }
        }
        response.setListChannelsReply(reply);
    }

    private static void handlePublish(Connection database, ZMQ.Socket publications,
                                      PublishRequest request, Response.Builder response)
            throws SQLException {
        String channel = request.getChannel();
        try (PreparedStatement exists = database.prepareStatement(
                "SELECT 1 FROM channels WHERE name=?")) {
            exists.setString(1, channel);
            try (ResultSet row = exists.executeQuery()) {
                if (!row.next()) {
                    response.setPublishReply(PublishReply.newBuilder()
                            .setStatus(404)
                            .setTimestamp(TimeUtil.now()));
                    return;
                }
            }
        }

        var sentAt = TimeUtil.now();
        String sql = "INSERT INTO publications(channel,sender,body,sent_at) VALUES(?,?,?,?)";
        try (PreparedStatement statement = database.prepareStatement(sql)) {
            statement.setString(1, channel);
            statement.setString(2, request.getUser());
            statement.setString(3, request.getMessage());
            statement.setTimestamp(4, java.sql.Timestamp.from(TimeUtil.toInstant(sentAt)));
            statement.executeUpdate();
        }

        Publication event = Publication.newBuilder()
                .setChannel(channel)
                .setSender(request.getUser())
                .setMessage(request.getMessage())
                .setTimestamp(sentAt)
                .setClock(TimeUtil.incrementLogicalClock())
                .build();
        publications.sendMore(channel);
        publications.send(event.toByteArray(), 0);

        response.setPublishReply(PublishReply.newBuilder()
                .setStatus(200)
                .setTimestamp(TimeUtil.now()));
    }
}
