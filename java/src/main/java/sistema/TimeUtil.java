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
    private static long logicalClock;
    private static long physicalOffsetNanos;

    private TimeUtil() {}

    static synchronized Timestamp now() {
        Instant instant = Instant.now().plusNanos(physicalOffsetNanos);
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    static synchronized long incrementLogicalClock() {
        return ++logicalClock;
    }

    static synchronized void observeLogicalClock(long received) {
        logicalClock = Math.max(logicalClock, received);
    }

    static synchronized long currentLogicalClock() {
        return logicalClock;
    }

    static synchronized double physicalOffsetMillis() {
        return physicalOffsetNanos / 1_000_000.0;
    }

    static synchronized void synchronizePhysicalClock(Timestamp referenceTime, Instant sentAt,
                                                       Instant receivedAt) {
        long roundTripNanos = java.time.Duration.between(sentAt, receivedAt).toNanos();
        Instant estimatedReferenceAtReceive = toInstant(referenceTime).plusNanos(roundTripNanos / 2);
        physicalOffsetNanos = java.time.Duration.between(receivedAt,
                estimatedReferenceAtReceive).toNanos();
        System.out.printf("relógio físico sincronizado; diferença local: %.3f ms%n",
                physicalOffsetNanos / 1_000_000.0);
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
