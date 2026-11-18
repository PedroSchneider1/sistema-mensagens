package sistema;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import sistema.proto.HeartbeatRequest;
import sistema.proto.ReferenceRequest;
import sistema.proto.ReferenceResponse;
import sistema.proto.RankRequest;
import sistema.proto.ServerListRequest;
import sistema.proto.ServerInfo;

import java.time.Instant;
import java.util.List;

/** Req-Rep client used by servers to register and heartbeat with the reference service. */
final class ReferenceClient implements AutoCloseable {
    private static final long RESPONSE_TIMEOUT_MILLIS = 2000;
    private final ZContext context;
    private ZMQ.Socket requests;

    ReferenceClient(ZContext context) {
        this.context = context;
        connect();
    }

    private void connect() {
        requests = context.createSocket(ZMQ.REQ);
        requests.setLinger(0);
        requests.connect("tcp://reference:5559");
    }

    int register(String serverName) throws Exception {
        while (true) {
            RankRequest rank = RankRequest.newBuilder().setName(serverName).build();
            ReferenceResponse response = call(ReferenceRequest.newBuilder().setRank(rank).build());
            if (response == null) {
                System.out.println("aguardando serviço de referência");
                continue;
            }
            if (response.hasRankReply()) {
                return response.getRankReply().getRank();
            }
        }
    }

    List<ServerInfo> listServers() throws Exception {
        while (true) {
            ReferenceResponse response = call(ReferenceRequest.newBuilder()
                    .setList(ServerListRequest.getDefaultInstance())
                    .build());
            if (response == null) {
                continue;
            }
            if (response.hasListReply()) {
                return response.getListReply().getServersList();
            }
        }
    }

    int heartbeat(String serverName) throws Exception {
        HeartbeatRequest heartbeat = HeartbeatRequest.newBuilder()
                .setName(serverName)
                .build();
        ReferenceResponse response = call(ReferenceRequest.newBuilder()
                .setHeartbeat(heartbeat)
                .build());
        return response == null || !response.hasHeartbeatReply()
                ? -1 : response.getHeartbeatReply().getStatus();
    }

    private ReferenceResponse call(ReferenceRequest request) throws Exception {
        ReferenceRequest stamped = request.toBuilder()
                .setTimestamp(TimeUtil.now())
                .setClock(TimeUtil.incrementLogicalClock())
                .build();
        Instant sentAt = Instant.now();
        requests.send(stamped.toByteArray(), 0);
        ZMQ.Poller poller = context.createPoller(1);
        poller.register(requests, ZMQ.Poller.POLLIN);
        if (poller.poll(RESPONSE_TIMEOUT_MILLIS) <= 0 || !poller.pollin(0)) {
            requests.close();
            connect();
            return null;
        }
        byte[] rawResponse = requests.recv(0);
        Instant receivedAt = Instant.now();
        ReferenceResponse response = ReferenceResponse.parseFrom(rawResponse);
        TimeUtil.observeLogicalClock(response.getClock());
        TimeUtil.synchronizePhysicalClock(response.getTimestamp(), sentAt, receivedAt);
        return response;
    }

    @Override
    public void close() {
        requests.close();
    }
}
