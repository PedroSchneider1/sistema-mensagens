package sistema;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import sistema.proto.ConnReply;
import sistema.proto.ClockRequest;
import sistema.proto.CoordinatorAnnouncement;
import sistema.proto.CreateChannelReply;
import sistema.proto.ElectionReply;
import sistema.proto.ElectionRequest;
import sistema.proto.ErrorReply;
import sistema.proto.ListChannelsReply;
import sistema.proto.LoginReply;
import sistema.proto.Publication;
import sistema.proto.PublishReply;
import sistema.proto.PublishRequest;
import sistema.proto.Request;
import sistema.proto.Response;
import sistema.proto.ServerInfo;
import sistema.proto.ServerRequest;
import sistema.proto.ServerResponse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Handles Req-Rep requests, Pub-Sub publications, and server-side persistence. */
final class Server {
    private static final Set<String> USERS = Set.of("pedro_client", "gabriel_client");
    private static final int HEARTBEAT_EVERY = 10;
    private static final int SYNC_EVERY = 15;
    private static final long PEER_TIMEOUT_MILLIS = 3000;
    private static String serverName;
    private static String serverAddress;
    private static int rank;
    private static String coordinator;
    private static boolean electionPending;

    private Server() {}

    static void run() throws Exception {
        try (Connection database = Database.connect(); ZContext context = new ZContext()) {
            Database.initializeSchema(database);

            serverName = System.getenv().getOrDefault("SERVER_NAME", "server");
            int peerPort = Integer.parseInt(System.getenv().getOrDefault("PEER_PORT", "5560"));
            serverAddress = System.getenv().getOrDefault("SERVER_ADDRESS",
                    "tcp://" + serverName + ":" + peerPort);
            try (ReferenceClient reference = new ReferenceClient(context)) {
                rank = reference.register(serverName, serverAddress);
                System.out.printf("servidor '%s' registrado com rank %d (%s)%n",
                        serverName, rank, serverAddress);

                ZMQ.Socket replies = context.createSocket(ZMQ.REP);
                replies.connect("tcp://broker:5556");
                ZMQ.Socket publications = context.createSocket(ZMQ.PUB);
                publications.connect("tcp://proxy:5557");
                ZMQ.Socket peerReplies = context.createSocket(ZMQ.REP);
                peerReplies.bind("tcp://*:" + peerPort);
                ZMQ.Socket announcements = context.createSocket(ZMQ.SUB);
                announcements.connect("tcp://proxy:5558");
                announcements.subscribe("servers");

                ZMQ.Poller poller = context.createPoller(3);
                poller.register(replies, ZMQ.Poller.POLLIN);
                poller.register(peerReplies, ZMQ.Poller.POLLIN);
                poller.register(announcements, ZMQ.Poller.POLLIN);
                int receivedRequests = 0;

                startElection(context, reference, publications);
                while (!Thread.currentThread().isInterrupted()) {
                    poller.poll(-1);

                    if (poller.pollin(0)) {
                        byte[] rawRequest = replies.recv(0);
                        Request request = Request.parseFrom(rawRequest);
                        TimeUtil.observeLogicalClock(request.getClock());
                        var requestTimestamp = request.hasTimestamp() ? request.getTimestamp() : TimeUtil.now();
                        System.out.printf("[%s] recebido: %s | relógio lógico: %d%n",
                                TimeUtil.format(requestTimestamp), request.getPayloadCase(),
                                TimeUtil.currentLogicalClock());

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

                        receivedRequests++;
                        if (receivedRequests % HEARTBEAT_EVERY == 0) {
                            sendHeartbeat(reference);
                        }
                        if (receivedRequests % SYNC_EVERY == 0) {
                            synchronizeClock(context, reference, publications);
                        }
                    }

                    if (poller.pollin(1)) {
                        electionPending = handlePeerRequest(peerReplies);
                    }
                    if (poller.pollin(2)) {
                        handleAnnouncement(announcements);
                    }
                    if (electionPending) {
                        electionPending = false;
                        startElection(context, reference, publications);
                    }
                }
            }
        }
    }

    private static void sendHeartbeat(ReferenceClient reference) throws Exception {
        int status = reference.heartbeat(serverName);
        if (status == 404) {
            rank = reference.register(serverName, serverAddress);
            System.out.printf("registrado novamente com rank %d%n", rank);
        } else if (status == 200) {
            System.out.printf("heartbeat ok | ajuste: %+.3f ms%n", TimeUtil.physicalOffsetMillis());
        } else {
            System.out.println("heartbeat sem resposta da referência");
        }
    }

    private static void synchronizeClock(ZContext context, ReferenceClient reference,
                                         ZMQ.Socket publications) throws Exception {
        if (serverName.equals(coordinator)) {
            return;
        }
        if (coordinator == null) {
            startElection(context, reference, publications);
            return;
        }

        List<ServerInfo> servers = reference.listServers();
        String address = servers.stream()
                .filter(server -> server.getName().equals(coordinator))
                .map(ServerInfo::getAddress)
                .findFirst()
                .orElse(null);
        PeerCallResult result = address == null ? null : callPeer(context, address,
                ServerRequest.newBuilder().setClockRequest(ClockRequest.getDefaultInstance()).build());
        if (result == null) {
            startElection(context, reference, publications);
            return;
        }

        TimeUtil.synchronizePhysicalClock(result.response().getTimestamp(),
                result.sentAt(), result.receivedAt());
        System.out.printf("relógio sincronizado com '%s' | ajuste: %+.3f ms%n",
                coordinator, TimeUtil.physicalOffsetMillis());
    }

    private static void startElection(ZContext context, ReferenceClient reference,
                                      ZMQ.Socket publications) throws Exception {
        System.out.println("iniciando eleição");
        List<ServerInfo> servers;
        try {
            servers = reference.listServers();
        } catch (Exception e) {
            coordinator = null;
            System.out.println("eleição adiada: referência indisponível");
            return;
        }

        for (ServerInfo server : servers) {
            if (server.getRank() <= rank) {
                continue;
            }
            ElectionRequest election = ElectionRequest.newBuilder()
                    .setName(serverName)
                    .setRank(rank)
                    .build();
            PeerCallResult result = callPeer(context, server.getAddress(),
                    ServerRequest.newBuilder().setElection(election).build());
            if (result != null && result.response().hasElectionReply()
                    && result.response().getElectionReply().getStatus().equals("OK")) {
                coordinator = null;
                System.out.printf("'%s' respondeu à eleição: aguardando anúncio%n", server.getName());
                return;
            }
        }

        if (publications != null) {
            announceCoordinator(publications);
        }
    }

    private static PeerCallResult callPeer(ZContext context, String address, ServerRequest request)
            throws Exception {
        ZMQ.Socket peer = context.createSocket(ZMQ.REQ);
        peer.setLinger(0);
        try {
            peer.connect(address);
            ServerRequest stamped = request.toBuilder()
                    .setTimestamp(TimeUtil.now())
                    .setClock(TimeUtil.incrementLogicalClock())
                    .build();
            Instant sentAt = Instant.now();
            peer.send(stamped.toByteArray(), 0);

            ZMQ.Poller peerPoller = context.createPoller(1);
            peerPoller.register(peer, ZMQ.Poller.POLLIN);
            if (peerPoller.poll(PEER_TIMEOUT_MILLIS) <= 0 || !peerPoller.pollin(0)) {
                return null;
            }

            ServerResponse response = ServerResponse.parseFrom(peer.recv(0));
            Instant receivedAt = Instant.now();
            TimeUtil.observeLogicalClock(response.getClock());
            return new PeerCallResult(response, sentAt, receivedAt);
        } catch (org.zeromq.ZMQException e) {
            System.out.printf("peer indisponível (%s): %s%n", address, e.getMessage());
            return null;
        } finally {
            peer.close();
        }
    }

    private static void announceCoordinator(ZMQ.Socket publications) {
        coordinator = serverName;
        CoordinatorAnnouncement announcement = CoordinatorAnnouncement.newBuilder()
                .setName(serverName)
                .setTimestamp(TimeUtil.now())
                .setClock(TimeUtil.incrementLogicalClock())
                .build();
        publications.sendMore("servers");
        publications.send(announcement.toByteArray(), 0);
        System.out.printf("eleito coordenador | relógio lógico: %d%n",
                TimeUtil.currentLogicalClock());
    }

    private static boolean handlePeerRequest(ZMQ.Socket peerReplies) throws Exception {
        ServerRequest request = ServerRequest.parseFrom(peerReplies.recv(0));
        TimeUtil.observeLogicalClock(request.getClock());
        System.out.printf("servidor: recebido '%s' | relógio lógico: %d%n",
                request.getPayloadCase(), TimeUtil.currentLogicalClock());

        ServerResponse.Builder response = ServerResponse.newBuilder();
        if (request.hasClockRequest()) {
            response.setClockReply(sistema.proto.ClockReply.getDefaultInstance());
        } else if (request.hasElection()) {
            response.setElectionReply(ElectionReply.newBuilder().setStatus("OK"));
        }
        response.setTimestamp(TimeUtil.now())
                .setClock(TimeUtil.incrementLogicalClock());
        peerReplies.send(response.build().toByteArray(), 0);
        return request.hasElection();
    }

    private static void handleAnnouncement(ZMQ.Socket announcements) throws Exception {
        announcements.recvStr(0);
        CoordinatorAnnouncement announcement = CoordinatorAnnouncement.parseFrom(announcements.recv(0));
        TimeUtil.observeLogicalClock(announcement.getClock());
        coordinator = announcement.getName();
        System.out.printf("coordenador: %s | relógio lógico: %d%n",
                coordinator, TimeUtil.currentLogicalClock());
    }

    private record PeerCallResult(ServerResponse response, Instant sentAt, Instant receivedAt) {}

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
