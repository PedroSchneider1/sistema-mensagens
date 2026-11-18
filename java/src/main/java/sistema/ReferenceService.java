package sistema;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import sistema.proto.HeartbeatReply;
import sistema.proto.RankReply;
import sistema.proto.ReferenceRequest;
import sistema.proto.ReferenceResponse;
import sistema.proto.ServerInfo;
import sistema.proto.ServerListReply;

import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

/** Service that assigns ranks and tracks active servers through heartbeats. */
final class ReferenceService {
    private static final long HEARTBEAT_TIMEOUT_SECONDS = 30;
    private static final Map<String, Integer> RANKS = new LinkedHashMap<>();
    private static final Map<String, String> ADDRESSES = new LinkedHashMap<>();
    private static final Map<String, Instant> LAST_SEEN = new LinkedHashMap<>();

    private ReferenceService() {}

    static void run() throws Exception {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket service = context.createSocket(ZMQ.REP);
            service.bind("tcp://*:5559");
            ZMQ.Poller poller = context.createPoller(1);
            poller.register(service, ZMQ.Poller.POLLIN);
            System.out.println("referência: REP 5559 (heartbeat timeout 30s)");

            while (!Thread.currentThread().isInterrupted()) {
                if (poller.poll(1000) == 0 || !poller.pollin(0)) {
                    removeExpiredServers();
                    continue;
                }

                ReferenceRequest request = ReferenceRequest.parseFrom(service.recv(0));
                removeExpiredServers();
                TimeUtil.observeLogicalClock(request.getClock());
                System.out.printf("recebido: %s | relógio lógico: %d%n",
                        request.getPayloadCase(), TimeUtil.currentLogicalClock());

                ReferenceResponse response = handle(request).toBuilder()
                        .setTimestamp(TimeUtil.now())
                        .setClock(TimeUtil.incrementLogicalClock())
                        .build();
                service.send(response.toByteArray(), 0);
            }
        }
    }

    private static synchronized ReferenceResponse handle(ReferenceRequest request) {
        ReferenceResponse.Builder response = ReferenceResponse.newBuilder();
        switch (request.getPayloadCase()) {
            case RANK -> {
                String name = request.getRank().getName();
                int rank = RANKS.computeIfAbsent(name, ignored -> RANKS.size() + 1);
                ADDRESSES.put(name, request.getRank().getAddress());
                LAST_SEEN.put(name, Instant.now());
                System.out.printf("servidor '%s' registrado com rank %d (%s)%n",
                        name, rank, request.getRank().getAddress());
                response.setRankReply(RankReply.newBuilder().setRank(rank));
            }
            case LIST -> {
                ServerListReply.Builder servers = ServerListReply.newBuilder();
                LAST_SEEN.keySet().stream()
                        .sorted(Comparator.comparingInt(RANKS::get))
                        .forEach(name -> servers.addServers(ServerInfo.newBuilder()
                                .setName(name)
                                .setRank(RANKS.get(name))
                                .setAddress(ADDRESSES.getOrDefault(name, ""))));
                response.setListReply(servers);
            }
            case HEARTBEAT -> {
                String name = request.getHeartbeat().getName();
                boolean isAvailable = LAST_SEEN.containsKey(name);
                if (isAvailable) {
                    LAST_SEEN.put(name, Instant.now());
                }
                int status = isAvailable ? 200 : 404;
                response.setHeartbeatReply(HeartbeatReply.newBuilder().setStatus(status));
                if (isAvailable) {
                    System.out.printf("heartbeat: %s (rank %d)%n", name, RANKS.get(name));
                }
            }
            case PAYLOAD_NOT_SET -> System.out.println("Mensagem desconhecida: null");
        }
        return response.build();
    }

    private static synchronized void removeExpiredServers() {
        Instant threshold = Instant.now().minusSeconds(HEARTBEAT_TIMEOUT_SECONDS);
        var iterator = LAST_SEEN.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Instant> entry = iterator.next();
            if (entry.getValue().isBefore(threshold)) {
                System.out.printf("servidor '%s' removido (sem heartbeat)%n", entry.getKey());
                iterator.remove();
            }
        }
    }
}
