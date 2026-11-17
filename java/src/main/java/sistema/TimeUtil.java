package sistema;

import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** Helpers for Protobuf timestamps and local log formatting. */
final class TimeUtil {
    private static final DateTimeFormatter CLOCK = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    private TimeUtil() {}

    static Timestamp now() {
        Instant instant = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    static String format(Timestamp timestamp) {
        return CLOCK.format(toInstant(timestamp));
    }

    static String format(Instant instant) {
        return CLOCK.format(instant);
    }
}
