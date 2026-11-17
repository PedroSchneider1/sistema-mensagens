package sistema;

import com.google.protobuf.Timestamp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/** PostgreSQL connection and persistence used by the server. */
final class Database {
    private Database() {}

    static Connection connect() throws InterruptedException {
        String host = System.getenv().getOrDefault("POSTGRES_HOST", "postgres");
        String database = System.getenv().getOrDefault("POSTGRES_DB", "messaging");
        String user = System.getenv().getOrDefault("POSTGRES_USER", "messaging");
        String password = System.getenv().getOrDefault("POSTGRES_PASSWORD", "messaging");
        String url = "jdbc:postgresql://" + host + ":5432/" + database;

        while (true) {
            try {
                return DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                System.out.println("aguardando postgres: " + e.getMessage());
                Thread.sleep(2000);
            }
        }
    }

    static void initializeSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS messages ("
                    + "id BIGSERIAL PRIMARY KEY, direction TEXT NOT NULL "
                    + "CHECK (direction IN ('in','out')), kind TEXT NOT NULL, "
                    + "payload BYTEA NOT NULL, message_ts TIMESTAMPTZ, "
                    + "created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
            statement.execute("CREATE TABLE IF NOT EXISTS login_events ("
                    + "id BIGSERIAL PRIMARY KEY, username TEXT NOT NULL, "
                    + "logged_at TIMESTAMPTZ NOT NULL)");
            statement.execute("CREATE TABLE IF NOT EXISTS channels ("
                    + "name TEXT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
            statement.execute("CREATE TABLE IF NOT EXISTS publications ("
                    + "id BIGSERIAL PRIMARY KEY, channel TEXT NOT NULL REFERENCES channels(name), "
                    + "sender TEXT NOT NULL, body TEXT NOT NULL, sent_at TIMESTAMPTZ NOT NULL, "
                    + "created_at TIMESTAMPTZ NOT NULL DEFAULT now())");
        }
    }

    static void saveMessage(Connection connection, String direction, String kind,
                            byte[] payload, Timestamp timestamp) throws SQLException {
        String sql = "INSERT INTO messages(direction,kind,payload,message_ts) VALUES(?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, direction);
            statement.setString(2, kind);
            statement.setBytes(3, payload);
            statement.setTimestamp(4, java.sql.Timestamp.from(TimeUtil.toInstant(timestamp)));
            statement.executeUpdate();
        }
    }
}
