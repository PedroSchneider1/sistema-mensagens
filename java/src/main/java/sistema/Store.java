package sistema;

import com.google.protobuf.Timestamp;
import sistema.proto.ChannelCreated;
import sistema.proto.HistoryEntry;
import sistema.proto.LogEntry;
import sistema.proto.MessagePublished;
import sistema.proto.ReplicaEntry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Per-server SQLite store for state replicas and the replicated client event log. */
final class Store implements AutoCloseable {
    static final int SYNC_PAGE = 500;

    private static final String[] SCHEMA = {
            """
            CREATE TABLE IF NOT EXISTS entries (
                origin TEXT NOT NULL,
                seq INTEGER NOT NULL,
                kind TEXT NOT NULL CHECK (kind IN ('channel', 'publication')),
                channel TEXT NOT NULL,
                sender TEXT,
                body TEXT,
                clock INTEGER NOT NULL,
                ts_ns INTEGER NOT NULL,
                PRIMARY KEY (origin, seq)
            )
            """,
            "CREATE INDEX IF NOT EXISTS entries_channel ON entries (channel, clock, origin, seq)",
            """
            CREATE TABLE IF NOT EXISTS message_log (
                origin TEXT NOT NULL,
                seq INTEGER NOT NULL,
                direction TEXT NOT NULL CHECK (direction IN ('in', 'out')),
                kind TEXT NOT NULL,
                payload BLOB NOT NULL,
                message_ts_ns INTEGER,
                clock INTEGER NOT NULL,
                PRIMARY KEY (origin, seq)
            )
            """
    };

    private final String origin;
    private final Connection connection;
    private final Map<String, Long> vector = new LinkedHashMap<>();
    private final Map<String, Long> logVector = new LinkedHashMap<>();

    Store(String databasePath, String origin) throws Exception {
        this.origin = origin;
        Path path = Path.of(databasePath);
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Class.forName("org.sqlite.JDBC");
        connection = DriverManager.getConnection("jdbc:sqlite:" + path);
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA synchronous=NORMAL");
            for (String sql : SCHEMA) {
                statement.execute(sql);
            }
        }
        loadVector("entries", vector);
        loadVector("message_log", logVector);
    }

    synchronized Map<String, Long> vector() {
        return new LinkedHashMap<>(vector);
    }

    synchronized Map<String, Long> logVector() {
        return new LinkedHashMap<>(logVector);
    }

    synchronized boolean covers(Map<String, Long> required) {
        return required.entrySet().stream()
                .allMatch(entry -> vector.getOrDefault(entry.getKey(), 0L) >= entry.getValue());
    }

    synchronized boolean channelExists(String channel) throws SQLException {
        String sql = "SELECT 1 FROM entries WHERE kind='channel' AND channel=? LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, channel);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    synchronized List<String> channels() throws SQLException {
        List<String> channels = new ArrayList<>();
        String sql = "SELECT DISTINCT channel FROM entries WHERE kind='channel' ORDER BY channel";
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                channels.add(result.getString(1));
            }
        }
        return channels;
    }

    synchronized List<HistoryEntry> history(String channel) throws SQLException {
        List<HistoryEntry> messages = new ArrayList<>();
        String sql = "SELECT origin,seq,sender,body,clock,ts_ns FROM entries "
                + "WHERE kind='publication' AND channel=? ORDER BY clock,origin,seq";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, channel);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    messages.add(HistoryEntry.newBuilder()
                            .setOrigin(result.getString("origin"))
                            .setSeq(result.getLong("seq"))
                            .setSender(result.getString("sender"))
                            .setMessage(result.getString("body"))
                            .setClock(result.getLong("clock"))
                            .setTimestamp(fromNanos(result.getLong("ts_ns")))
                            .build());
                }
            }
        }
        return messages;
    }

    synchronized ReplicaEntry appendChannel(String name, long clock, Timestamp timestamp)
            throws SQLException {
        ReplicaEntry entry = ReplicaEntry.newBuilder()
                .setOrigin(origin)
                .setSeq(vector.getOrDefault(origin, 0L) + 1)
                .setClock(clock)
                .setTimestamp(timestamp)
                .setChannel(ChannelCreated.newBuilder().setName(name))
                .build();
        insert(entry);
        return entry;
    }

    synchronized ReplicaEntry appendPublication(String channel, String sender, String message,
                                                long clock, Timestamp timestamp) throws SQLException {
        ReplicaEntry entry = ReplicaEntry.newBuilder()
                .setOrigin(origin)
                .setSeq(vector.getOrDefault(origin, 0L) + 1)
                .setClock(clock)
                .setTimestamp(timestamp)
                .setPublication(MessagePublished.newBuilder()
                        .setChannel(channel)
                        .setSender(sender)
                        .setMessage(message))
                .build();
        insert(entry);
        return entry;
    }

    synchronized ApplyResult apply(ReplicaEntry entry) throws SQLException {
        long last = vector.getOrDefault(entry.getOrigin(), 0L);
        if (entry.getSeq() <= last) {
            return ApplyResult.DUPLICATE;
        }
        if (entry.getSeq() != last + 1) {
            return ApplyResult.GAP;
        }
        insert(entry);
        return ApplyResult.APPLIED;
    }

    synchronized List<ReplicaEntry> entriesSince(Map<String, Long> peerVector) throws SQLException {
        List<ReplicaEntry> entries = new ArrayList<>();
        String sql = "SELECT origin,seq,kind,channel,sender,body,clock,ts_ns FROM entries "
                + "WHERE origin=? AND seq>? ORDER BY seq LIMIT ?";
        for (String entryOrigin : vector.keySet()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, entryOrigin);
                statement.setLong(2, peerVector.getOrDefault(entryOrigin, 0L));
                statement.setInt(3, SYNC_PAGE - entries.size());
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        entries.add(readEntry(result));
                    }
                }
            }
            if (entries.size() >= SYNC_PAGE) {
                break;
            }
        }
        return entries;
    }

    synchronized LogEntry appendLog(String direction, String kind, byte[] payload,
                                    Timestamp messageTimestamp, long clock) throws SQLException {
        LogEntry.Builder entry = LogEntry.newBuilder()
                .setOrigin(origin)
                .setSeq(logVector.getOrDefault(origin, 0L) + 1)
                .setDirection(direction)
                .setKind(kind)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .setClock(clock);
        if (messageTimestamp != null) {
            entry.setMessageTs(messageTimestamp);
        }
        LogEntry built = entry.build();
        insertLog(built);
        return built;
    }

    synchronized ApplyResult applyLog(LogEntry entry) throws SQLException {
        long last = logVector.getOrDefault(entry.getOrigin(), 0L);
        if (entry.getSeq() <= last) {
            return ApplyResult.DUPLICATE;
        }
        if (entry.getSeq() != last + 1) {
            return ApplyResult.GAP;
        }
        insertLog(entry);
        return ApplyResult.APPLIED;
    }

    synchronized List<LogEntry> logEntriesSince(Map<String, Long> peerLogVector) throws SQLException {
        List<LogEntry> entries = new ArrayList<>();
        String sql = "SELECT origin,seq,direction,kind,payload,message_ts_ns,clock FROM message_log "
                + "WHERE origin=? AND seq>? ORDER BY seq LIMIT ?";
        for (String entryOrigin : logVector.keySet()) {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, entryOrigin);
                statement.setLong(2, peerLogVector.getOrDefault(entryOrigin, 0L));
                statement.setInt(3, SYNC_PAGE - entries.size());
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        entries.add(readLog(result));
                    }
                }
            }
            if (entries.size() >= SYNC_PAGE) {
                break;
            }
        }
        return entries;
    }

    private void loadVector(String table, Map<String, Long> target) throws SQLException {
        String sql = "SELECT origin,MAX(seq) FROM " + table + " GROUP BY origin";
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                target.put(result.getString(1), result.getLong(2));
            }
        }
    }

    private void insert(ReplicaEntry entry) throws SQLException {
        String kind = entry.hasChannel() ? "channel" : "publication";
        String channel = entry.hasChannel() ? entry.getChannel().getName()
                : entry.getPublication().getChannel();
        String sender = entry.hasPublication() ? entry.getPublication().getSender() : null;
        String body = entry.hasPublication() ? entry.getPublication().getMessage() : null;
        String sql = "INSERT INTO entries(origin,seq,kind,channel,sender,body,clock,ts_ns) "
                + "VALUES(?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, entry.getOrigin());
            statement.setLong(2, entry.getSeq());
            statement.setString(3, kind);
            statement.setString(4, channel);
            statement.setString(5, sender);
            statement.setString(6, body);
            statement.setLong(7, entry.getClock());
            statement.setLong(8, toNanos(entry.getTimestamp()));
            statement.executeUpdate();
        }
        vector.put(entry.getOrigin(), entry.getSeq());
    }

    private static ReplicaEntry readEntry(ResultSet result) throws SQLException {
        String kind = result.getString("kind");
        ReplicaEntry.Builder entry = ReplicaEntry.newBuilder()
                .setOrigin(result.getString("origin"))
                .setSeq(result.getLong("seq"))
                .setClock(result.getLong("clock"))
                .setTimestamp(fromNanos(result.getLong("ts_ns")));
        if (kind.equals("channel")) {
            entry.setChannel(ChannelCreated.newBuilder().setName(result.getString("channel")));
        } else {
            entry.setPublication(MessagePublished.newBuilder()
                    .setChannel(result.getString("channel"))
                    .setSender(result.getString("sender"))
                    .setMessage(result.getString("body")));
        }
        return entry.build();
    }

    private void insertLog(LogEntry entry) throws SQLException {
        String sql = "INSERT INTO message_log(origin,seq,direction,kind,payload,message_ts_ns,clock) "
                + "VALUES(?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, entry.getOrigin());
            statement.setLong(2, entry.getSeq());
            statement.setString(3, entry.getDirection());
            statement.setString(4, entry.getKind());
            statement.setBytes(5, entry.getPayload().toByteArray());
            if (entry.hasMessageTs()) {
                statement.setLong(6, toNanos(entry.getMessageTs()));
            } else {
                statement.setNull(6, Types.BIGINT);
            }
            statement.setLong(7, entry.getClock());
            statement.executeUpdate();
        }
        logVector.put(entry.getOrigin(), entry.getSeq());
    }

    private static LogEntry readLog(ResultSet result) throws SQLException {
        LogEntry.Builder entry = LogEntry.newBuilder()
                .setOrigin(result.getString("origin"))
                .setSeq(result.getLong("seq"))
                .setDirection(result.getString("direction"))
                .setKind(result.getString("kind"))
                .setPayload(com.google.protobuf.ByteString.copyFrom(result.getBytes("payload")))
                .setClock(result.getLong("clock"));
        long messageTs = result.getLong("message_ts_ns");
        if (!result.wasNull()) {
            entry.setMessageTs(fromNanos(messageTs));
        }
        return entry.build();
    }

    private static long toNanos(Timestamp timestamp) {
        return timestamp.getSeconds() * 1_000_000_000L + timestamp.getNanos();
    }

    private static Timestamp fromNanos(long nanos) {
        return Timestamp.newBuilder()
                .setSeconds(Math.floorDiv(nanos, 1_000_000_000L))
                .setNanos((int) Math.floorMod(nanos, 1_000_000_000L))
                .build();
    }

    @Override
    public synchronized void close() throws SQLException {
        connection.close();
    }

    enum ApplyResult { APPLIED, DUPLICATE, GAP }
}
