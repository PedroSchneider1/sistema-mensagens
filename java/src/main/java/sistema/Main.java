package sistema;

import com.google.protobuf.ByteString;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import sistema.proto.*;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public final class Main {
    private static final Set<String> USERS = Set.of("pedro_client", "gabriel_client");
    private static final String[] WORDS = {"olá", "tudo", "bem", "aqui", "canal", "teste", "mensagem", "ok", "hoje", "dados", "bot", "sistema"};
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private static com.google.protobuf.Timestamp now() { Instant instant = Instant.now(); return com.google.protobuf.Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build(); }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("uso: java -jar app.jar server|client");
        switch (args[0]) {
            case "server" -> runServer();
            case "client" -> runClient();
            default -> throw new IllegalArgumentException("modo desconhecido: " + args[0]);
        }
    }

    private static Connection connectDb() throws InterruptedException {
        String host = System.getenv().getOrDefault("POSTGRES_HOST", "postgres");
        String db = System.getenv().getOrDefault("POSTGRES_DB", "messaging");
        String user = System.getenv().getOrDefault("POSTGRES_USER", "messaging");
        String password = System.getenv().getOrDefault("POSTGRES_PASSWORD", "messaging");
        String url = "jdbc:postgresql://" + host + ":5432/" + db;
        while (true) {
            try { return DriverManager.getConnection(url, user, password); }
            catch (SQLException e) { System.out.println("aguardando postgres: " + e.getMessage()); Thread.sleep(2000); }
        }
    }

    private static void runServer() throws Exception {
        try (Connection db = connectDb(); ZContext context = new ZContext()) {
            try (Statement s = db.createStatement()) {
                s.execute("CREATE TABLE IF NOT EXISTS messages (id BIGSERIAL PRIMARY KEY, direction TEXT NOT NULL CHECK (direction IN ('in','out')), kind TEXT NOT NULL, payload BYTEA NOT NULL, message_ts TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
                s.execute("CREATE TABLE IF NOT EXISTS login_events (id BIGSERIAL PRIMARY KEY, username TEXT NOT NULL, logged_at TIMESTAMPTZ NOT NULL)");
                s.execute("CREATE TABLE IF NOT EXISTS channels (name TEXT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
                s.execute("CREATE TABLE IF NOT EXISTS publications (id BIGSERIAL PRIMARY KEY, channel TEXT NOT NULL REFERENCES channels(name), sender TEXT NOT NULL, body TEXT NOT NULL, sent_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
            }
            ZMQ.Socket rep = context.createSocket(ZMQ.REP); rep.connect("tcp://broker:5556");
            ZMQ.Socket pub = context.createSocket(ZMQ.PUB); pub.connect("tcp://proxy:5557");
            while (!Thread.currentThread().isInterrupted()) {
                byte[] raw = rep.recv(0);
                Request req = Request.parseFrom(raw);
                com.google.protobuf.Timestamp reqTs = req.hasTimestamp() ? req.getTimestamp() : now();
                System.out.printf("[%s] recebido: %s%n", CLOCK.format(Instant.ofEpochSecond(reqTs.getSeconds(), reqTs.getNanos())), req.getPayloadCase());
                saveMessage(db, "in", req.getPayloadCase().name().toLowerCase(Locale.ROOT), raw, reqTs);
                Response response = handle(db, pub, req);
                response = response.toBuilder().setTimestamp(now()).build();
                saveMessage(db, "out", response.getPayloadCase().name().toLowerCase(Locale.ROOT), response.toByteArray(), response.getTimestamp());
                rep.send(response.toByteArray(), 0);
            }
        }
    }

    private static void saveMessage(Connection db, String direction, String kind, byte[] payload, com.google.protobuf.Timestamp ts) throws SQLException {
        try (PreparedStatement p = db.prepareStatement("INSERT INTO messages(direction,kind,payload,message_ts) VALUES(?,?,?,?)")) {
            p.setString(1, direction); p.setString(2, kind); p.setBytes(3, payload);
            p.setTimestamp(4, Timestamp.from(Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()))); p.executeUpdate();
        }
    }

    private static Response handle(Connection db, ZMQ.Socket pub, Request req) throws SQLException {
        Response.Builder out = Response.newBuilder();
        switch (req.getPayloadCase()) {
            case CONN -> out.setConnReply(ConnReply.newBuilder().setMessage("pong: " + req.getConn().getMessage()).setTimestamp(now()));
            case LOGIN -> {
                String name = req.getLogin().getUser(); System.out.println("Tentativa de login: " + name);
                int status = USERS.contains(name) ? 200 : 401;
                if (status == 200) try (PreparedStatement p = db.prepareStatement("INSERT INTO login_events(username,logged_at) VALUES(?,?)")) {
                    p.setString(1, name); p.setTimestamp(2, Timestamp.from(Instant.now())); p.executeUpdate();
                }
                out.setLoginReply(LoginReply.newBuilder().setStatus(status).setTimestamp(now()));
            }
            case CREATE_CHANNEL -> {
                try (PreparedStatement p = db.prepareStatement("INSERT INTO channels(name) VALUES(?) ON CONFLICT DO NOTHING")) {
                    p.setString(1, req.getCreateChannel().getName()); int status = p.executeUpdate() == 1 ? 200 : 409;
                    out.setCreateChannelReply(CreateChannelReply.newBuilder().setStatus(status).setTimestamp(now()));
                }
            }
            case LIST_CHANNELS -> {
                ListChannelsReply.Builder reply = ListChannelsReply.newBuilder().setTimestamp(now());
                try (Statement s = db.createStatement(); ResultSet rows = s.executeQuery("SELECT name FROM channels ORDER BY name")) { while (rows.next()) reply.addChannels(rows.getString(1)); }
                out.setListChannelsReply(reply);
            }
            case PUBLISH -> {
                PublishRequest p = req.getPublish();
                try (PreparedStatement exists = db.prepareStatement("SELECT 1 FROM channels WHERE name=?")) {
                    exists.setString(1, p.getChannel());
                    try (ResultSet row = exists.executeQuery()) {
                        if (!row.next()) out.setPublishReply(PublishReply.newBuilder().setStatus(404).setTimestamp(now()));
                        else {
                            com.google.protobuf.Timestamp sent = now();
                            try (PreparedStatement ins = db.prepareStatement("INSERT INTO publications(channel,sender,body,sent_at) VALUES(?,?,?,?)")) {
                                ins.setString(1,p.getChannel()); ins.setString(2,p.getUser()); ins.setString(3,p.getMessage());
                                ins.setTimestamp(4,Timestamp.from(Instant.ofEpochSecond(sent.getSeconds(),sent.getNanos()))); ins.executeUpdate();
                            }
                            Publication event = Publication.newBuilder().setChannel(p.getChannel()).setSender(p.getUser()).setMessage(p.getMessage()).setTimestamp(sent).build();
                            pub.sendMore(p.getChannel()); pub.send(event.toByteArray(), 0);
                            out.setPublishReply(PublishReply.newBuilder().setStatus(200).setTimestamp(now()));
                        }
                    }
                }
            }
            case PAYLOAD_NOT_SET -> out.setError(ErrorReply.newBuilder().setError("unknown command: empty").setTimestamp(now()));
        }
        return out.build();
    }

    private static void runClient() throws Exception {
        String user = System.getenv("BOT_USER");
        if (user == null || user.isBlank()) throw new IllegalArgumentException("BOT_USER não definido");
        try (ZContext context = new ZContext()) {
            ZMQ.Socket req = context.createSocket(ZMQ.REQ); req.connect("tcp://broker:5555");
            ZMQ.Socket sub = context.createSocket(ZMQ.SUB); sub.connect("tcp://proxy:5558");
            int loginStatus;
            do {
                Response response = call(req, Request.newBuilder().setLogin(LoginRequest.newBuilder().setUser(user).setTimestamp(now())).build());
                loginStatus = response.getLoginReply().getStatus();
                if (loginStatus != 200) { System.out.printf("falha no login de '%s' (status %d); tentando novamente%n", user, loginStatus); Thread.sleep(1000); }
            } while (loginStatus != 200);
            System.out.printf("logado como '%s'%n", user);
            List<String> channels = listChannels(req);
            while (channels.size() < 5) {
                String name = "canal_" + UUID.randomUUID().toString().replace("-", "").substring(0, 6);
                int status = call(req, Request.newBuilder().setCreateChannel(CreateChannelRequest.newBuilder().setName(name).setTimestamp(now())).build()).getCreateChannelReply().getStatus();
                System.out.printf("criar canal '%s': status %d%n", name, status); channels = listChannels(req);
            }
            Collections.shuffle(channels);
            for (int i = 0; i < Math.min(3, channels.size()); i++) { sub.subscribe(channels.get(i)); System.out.printf("inscrito no canal '%s'%n", channels.get(i)); }
            Thread.sleep(500);
            while (true) {
                channels = listChannels(req);
                String channel = channels.get(ThreadLocalRandom.current().nextInt(channels.size()));
                System.out.printf("--- enviando 10 mensagens para '%s'%n", channel);
                for (int i = 0; i < 10; i++) {
                    long start = System.nanoTime();
                    String message = randomMessage();
                    int status = call(req, Request.newBuilder().setPublish(PublishRequest.newBuilder().setUser(user).setChannel(channel).setMessage(message).setTimestamp(now())).build()).getPublishReply().getStatus();
                    if (status != 200) System.out.printf("erro ao publicar (status %d)%n", status);
                    pump(context, sub, Math.max(0, 1000 - (System.nanoTime() - start) / 1_000_000));
                }
            }
        }
    }

    private static Response call(ZMQ.Socket req, Request request) throws Exception {
        Request stamped = request.toBuilder().setTimestamp(now()).build();
        req.send(stamped.toByteArray(), 0);
        return Response.parseFrom(req.recv(0));
    }
    private static List<String> listChannels(ZMQ.Socket req) throws Exception {
        Response r = call(req, Request.newBuilder().setListChannels(ListChannelsRequest.newBuilder().setTimestamp(now())).build());
        return new ArrayList<>(r.getListChannelsReply().getChannelsList());
    }
    private static String randomMessage() {
        int count = ThreadLocalRandom.current().nextInt(3, 7); StringJoiner words = new StringJoiner(" ");
        for (int i = 0; i < count; i++) words.add(WORDS[ThreadLocalRandom.current().nextInt(WORDS.length)]); return words.toString();
    }
    private static void pump(ZContext context, ZMQ.Socket sub, long millis) throws Exception {
        long end = System.nanoTime() + millis * 1_000_000;
        ZMQ.Poller poller = context.createPoller(1);
        poller.register(sub, ZMQ.Poller.POLLIN);
        while (System.nanoTime() < end) {
            long left = Math.max(1, (end - System.nanoTime()) / 1_000_000);
            if (poller.poll(left) > 0 && poller.pollin(0)) {
                String topic = sub.recvStr(0); Publication event = Publication.parseFrom(sub.recv(0));
                if (!event.getChannel().equals(topic)) continue;
                System.out.printf("[%s] %s: %s | enviado: %s | recebido: %s%n", topic, event.getSender(), event.getMessage(),
                    CLOCK.format(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos())), CLOCK.format(Instant.now()));
            }
        }
    }
}
