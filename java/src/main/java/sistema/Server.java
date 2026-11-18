package sistema;

import com.google.protobuf.Timestamp;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import sistema.proto.ClockReply;
import sistema.proto.ClockRequest;
import sistema.proto.ConnReply;
import sistema.proto.CoordinatorAnnouncement;
import sistema.proto.CreateChannelReply;
import sistema.proto.ElectionReply;
import sistema.proto.ElectionRequest;
import sistema.proto.ErrorReply;
import sistema.proto.GetHistoryReply;
import sistema.proto.HistoryEntry;
import sistema.proto.ListChannelsReply;
import sistema.proto.LoginReply;
import sistema.proto.LogEntry;
import sistema.proto.Publication;
import sistema.proto.PublishReply;
import sistema.proto.PublishRequest;
import sistema.proto.ReplicaEntry;
import sistema.proto.Request;
import sistema.proto.Response;
import sistema.proto.ServerInfo;
import sistema.proto.ServerRequest;
import sistema.proto.ServerResponse;
import sistema.proto.SyncReply;
import sistema.proto.SyncRequest;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Multi-master server with active replication, repair, and session consistency. */
final class Server {
    private static final Set<String> USERS = Set.of("pedro_client", "gabriel_client");
    private static final int HEARTBEAT_EVERY = 10;
    private static final int SYNC_EVERY = 15;
    private static final long SESSION_WAIT_MILLIS = 3000;
    private static final long PEER_TIMEOUT_MILLIS = 3000;
    private static final long CATCH_UP_COOLDOWN_NANOS = 1_000_000_000L;

    private static String serverName;
    private static String serverAddress;
    private static int rank;
    private static String coordinator;
    private static boolean electionPending;
    private static long lastCatchUpNanos;

    private Server() {}

    static void run() throws Exception {
        serverName = System.getenv().getOrDefault("SERVER_NAME", "server");
        int peerPort = Integer.parseInt(System.getenv().getOrDefault("PEER_PORT", "5560"));
        serverAddress = System.getenv().getOrDefault("SERVER_ADDRESS",
                "tcp://" + serverName + ":" + peerPort);
        String databasePath = System.getenv().getOrDefault("DB_PATH", "/data/" + serverName + ".db");

        try (ZContext context = new ZContext();
             Store store = new Store(databasePath, serverName);
             ReferenceClient reference = new ReferenceClient(context)) {
            System.out.printf("armazenamento em %s | vetor: %s%n", databasePath, store.vector());

            ZMQ.Socket clientRequests = context.createSocket(ZMQ.REP);
            clientRequests.connect("tcp://broker:5556");
            ZMQ.Socket publications = context.createSocket(ZMQ.PUB);
            publications.connect("tcp://proxy:5557");
            ZMQ.Socket peerRequests = context.createSocket(ZMQ.REP);
            peerRequests.bind("tcp://*:" + peerPort);
            ZMQ.Socket subscriptions = context.createSocket(ZMQ.SUB);
            subscriptions.connect("tcp://proxy:5558");
            subscriptions.subscribe("servers");
            subscriptions.subscribe("replica");
            subscriptions.subscribe("eventlog");

            rank = reference.register(serverName, serverAddress);
            System.out.printf("servidor '%s' com rank %d (%s)%n", serverName, rank, serverAddress);

            catchUp(context, reference, peerRequests, subscriptions, publications, store);
            startElection(context, reference, peerRequests, subscriptions, publications, store);

            ZMQ.Poller poller = context.createPoller(3);
            poller.register(clientRequests, ZMQ.Poller.POLLIN);
            poller.register(peerRequests, ZMQ.Poller.POLLIN);
            poller.register(subscriptions, ZMQ.Poller.POLLIN);
            int receivedRequests = 0;

            while (!Thread.currentThread().isInterrupted()) {
                poller.poll(-1);

                if (poller.pollin(0)) {
                    serveClient(context, clientRequests, reference, peerRequests, subscriptions,
                            publications, store);
                    receivedRequests++;
                    if (receivedRequests % HEARTBEAT_EVERY == 0) {
                        sendHeartbeat(reference);
                        catchUp(context, reference, peerRequests, subscriptions, publications, store);
                    }
                    if (receivedRequests % SYNC_EVERY == 0) {
                        synchronizeClock(context, reference, peerRequests, subscriptions,
                                publications, store);
                    }
                }
                if (poller.pollin(1)) {
                    electionPending = servePeerRequest(peerRequests, store);
                }
                if (poller.pollin(2)) {
                    serveSubscription(context, reference, peerRequests, subscriptions,
                            publications, store);
                }
                if (electionPending) {
                    electionPending = false;
                    startElection(context, reference, peerRequests, subscriptions, publications, store);
                }
            }
        }
    }

    private static void serveClient(ZContext context, ZMQ.Socket clientRequests, ReferenceClient reference,
                                    ZMQ.Socket peerRequests, ZMQ.Socket subscriptions,
                                    ZMQ.Socket publications, Store store) throws Exception {
        byte[] rawRequest = clientRequests.recv(0);
        Request request = Request.parseFrom(rawRequest);
        TimeUtil.observeLogicalClock(request.getClock());
        Timestamp requestTimestamp = request.hasTimestamp() ? request.getTimestamp() : TimeUtil.now();
        String kind = request.getPayloadCase().name().toLowerCase(Locale.ROOT);
        System.out.printf("[%s] recebido: %s | relógio lógico: %d%n",
                TimeUtil.format(requestTimestamp), request.getPayloadCase(),
                TimeUtil.currentLogicalClock());

        logMessage(publications, store, "in", kind, rawRequest, requestTimestamp);
        Response response = handle(context, request, peerRequests, subscriptions, reference,
                publications, store);
        logMessage(publications, store, "out", response.getPayloadCase()
                .name().toLowerCase(Locale.ROOT), response.toByteArray(), response.getTimestamp());
        clientRequests.send(response.toByteArray(), 0);
    }

    private static Response handle(ZContext context, Request request, ZMQ.Socket peerRequests,
                                   ZMQ.Socket subscriptions, ReferenceClient reference,
                                   ZMQ.Socket publications, Store store) throws Exception {
        Response.Builder response;
        if (waitForVector(context, request.getVectorMap(), peerRequests, subscriptions, reference,
                publications, store)) {
            response = dispatch(request, publications, store).toBuilder();
            response.putAllVector(store.vector());
        } else {
            System.out.printf("réplica atrasada: vetor pedido %s, vetor local %s%n",
                    request.getVectorMap(), store.vector());
            response = Response.newBuilder().setError(ErrorReply.newBuilder()
                    .setError("replica behind")
                    .setStatus(503)
                    .setTimestamp(TimeUtil.now()));
        }

        return response.setTimestamp(TimeUtil.now())
                .setClock(TimeUtil.incrementLogicalClock())
                .build();
    }

    private static boolean waitForVector(ZContext context, Map<String, Long> required, ZMQ.Socket peerRequests,
                                         ZMQ.Socket subscriptions, ReferenceClient reference,
                                         ZMQ.Socket publications, Store store) throws Exception {
        if (store.covers(required)) {
            return true;
        }

        System.out.printf("aguardando réplica: cliente viu %s, servidor tem %s%n",
                required, store.vector());
        ZMQ.Poller poller = context.createPoller(2);
        poller.register(subscriptions, ZMQ.Poller.POLLIN);
        poller.register(peerRequests, ZMQ.Poller.POLLIN);
        long deadline = System.nanoTime() + SESSION_WAIT_MILLIS * 1_000_000;

        while (!store.covers(required)) {
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                return false;
            }
            long waitMillis = Math.max(1, (remainingNanos + 999_999) / 1_000_000);
            if (poller.poll(waitMillis) <= 0) {
                continue;
            }
            if (poller.pollin(0)) {
                serveSubscription(null, reference, peerRequests, subscriptions, publications, store);
            }
            if (poller.pollin(1)) {
                electionPending = servePeerRequest(peerRequests, store);
            }
        }
        return true;
    }

    private static Response dispatch(Request request, ZMQ.Socket publications, Store store)
            throws SQLException {
        Response.Builder response = Response.newBuilder();
        switch (request.getPayloadCase()) {
            case CONN -> response.setConnReply(ConnReply.newBuilder()
                    .setMessage("pong: " + request.getConn().getMessage())
                    .setTimestamp(TimeUtil.now()));
            case LOGIN -> {
                String user = request.getLogin().getUser();
                System.out.println("Tentativa de login: " + user);
                int status = USERS.contains(user) ? 200 : 401;
                response.setLoginReply(LoginReply.newBuilder()
                        .setStatus(status)
                        .setTimestamp(TimeUtil.now()));
            }
            case CREATE_CHANNEL -> response.setCreateChannelReply(
                    createChannel(request.getCreateChannel().getName(), publications, store));
            case LIST_CHANNELS -> response.setListChannelsReply(ListChannelsReply.newBuilder()
                    .addAllChannels(store.channels())
                    .setTimestamp(TimeUtil.now()));
            case PUBLISH -> response.setPublishReply(
                    publish(request.getPublish(), publications, store));
            case GET_HISTORY -> response.setGetHistoryReply(
                    getHistory(request.getGetHistory().getChannel(), store));
            case PAYLOAD_NOT_SET -> response.setError(ErrorReply.newBuilder()
                    .setError("unknown command: empty")
                    .setTimestamp(TimeUtil.now()));
        }
        return response.build();
    }

    private static CreateChannelReply createChannel(String name, ZMQ.Socket publications,
                                                   Store store) throws SQLException {
        int status;
        if (store.channelExists(name)) {
            status = 409;
        } else {
            ReplicaEntry entry = store.appendChannel(name,
                    TimeUtil.incrementLogicalClock(), TimeUtil.now());
            publishReplica(publications, entry);
            status = 200;
        }
        return CreateChannelReply.newBuilder().setStatus(status).setTimestamp(TimeUtil.now()).build();
    }

    private static PublishReply publish(PublishRequest request, ZMQ.Socket publications,
                                        Store store) throws SQLException {
        if (!store.channelExists(request.getChannel())) {
            return PublishReply.newBuilder().setStatus(404).setTimestamp(TimeUtil.now()).build();
        }

        Timestamp sentAt = TimeUtil.now();
        ReplicaEntry entry = store.appendPublication(request.getChannel(), request.getUser(),
                request.getMessage(), TimeUtil.incrementLogicalClock(), sentAt);
        publishReplica(publications, entry);

        Publication event = Publication.newBuilder()
                .setChannel(request.getChannel())
                .setSender(request.getUser())
                .setMessage(request.getMessage())
                .setTimestamp(sentAt)
                .setClock(TimeUtil.incrementLogicalClock())
                .build();
        sendTopic(publications, request.getChannel(), event.toByteArray());
        return PublishReply.newBuilder().setStatus(200).setTimestamp(TimeUtil.now()).build();
    }

    private static GetHistoryReply getHistory(String channel, Store store) throws SQLException {
        if (!store.channelExists(channel)) {
            return GetHistoryReply.newBuilder().setStatus(404).setTimestamp(TimeUtil.now()).build();
        }
        List<HistoryEntry> history = store.history(channel);
        return GetHistoryReply.newBuilder()
                .setStatus(200)
                .addAllMessages(history)
                .setTimestamp(TimeUtil.now())
                .build();
    }

    private static void logMessage(ZMQ.Socket publications, Store store, String direction,
                                   String kind, byte[] payload, Timestamp messageTimestamp)
            throws SQLException {
        LogEntry entry = store.appendLog(direction, kind, payload, messageTimestamp,
                TimeUtil.incrementLogicalClock());
        sendTopic(publications, "eventlog", entry.toByteArray());
    }

    private static void publishReplica(ZMQ.Socket publications, ReplicaEntry entry) {
        sendTopic(publications, "replica", entry.toByteArray());
    }

    private static void sendTopic(ZMQ.Socket publications, String topic, byte[] payload) {
        publications.sendMore(topic);
        publications.send(payload, 0);
    }

    private static void serveSubscription(ZContext context, ReferenceClient reference,
                                         ZMQ.Socket peerRequests, ZMQ.Socket subscriptions,
                                         ZMQ.Socket publications, Store store) throws Exception {
        byte[] topic = subscriptions.recv(0);
        byte[] payload = subscriptions.recv(0);
        if (java.util.Arrays.equals(topic, "servers".getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
            CoordinatorAnnouncement announcement = CoordinatorAnnouncement.parseFrom(payload);
            TimeUtil.observeLogicalClock(announcement.getClock());
            coordinator = announcement.getName();
            System.out.printf("coordenador: %s | relógio lógico: %d%n",
                    coordinator, TimeUtil.currentLogicalClock());
        } else if (java.util.Arrays.equals(topic, "replica".getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
            applyReplica(payload, context, reference, peerRequests, subscriptions, publications, store);
        } else if (java.util.Arrays.equals(topic, "eventlog".getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
            applyLog(payload, context, reference, peerRequests, subscriptions, publications, store);
        }
    }

    private static void applyReplica(byte[] payload, ZContext context, ReferenceClient reference,
                                     ZMQ.Socket peerRequests, ZMQ.Socket subscriptions,
                                     ZMQ.Socket publications, Store store) throws Exception {
        ReplicaEntry entry = ReplicaEntry.parseFrom(payload);
        if (entry.getOrigin().equals(serverName)) {
            return;
        }
        TimeUtil.observeLogicalClock(entry.getClock());
        Store.ApplyResult result = store.apply(entry);
        if (result == Store.ApplyResult.APPLIED) {
            System.out.printf("replicado: %s %s#%d | vetor: %s%n",
                    entry.getPayloadCase(), entry.getOrigin(), entry.getSeq(), store.vector());
        } else if (result == Store.ApplyResult.GAP) {
            System.out.printf("lacuna em '%s': esperado #%d, recebido #%d%n",
                    entry.getOrigin(), store.vector().getOrDefault(entry.getOrigin(), 0L) + 1,
                    entry.getSeq());
            catchUpOnGap(context, reference, peerRequests, subscriptions, publications, store);
        }
    }

    private static void applyLog(byte[] payload, ZContext context, ReferenceClient reference,
                                 ZMQ.Socket peerRequests, ZMQ.Socket subscriptions,
                                 ZMQ.Socket publications, Store store) throws Exception {
        LogEntry entry = LogEntry.parseFrom(payload);
        if (entry.getOrigin().equals(serverName)) {
            return;
        }
        TimeUtil.observeLogicalClock(entry.getClock());
        if (store.applyLog(entry) == Store.ApplyResult.GAP) {
            System.out.printf("lacuna no log de '%s': esperado #%d, recebido #%d%n",
                    entry.getOrigin(), store.logVector().getOrDefault(entry.getOrigin(), 0L) + 1,
                    entry.getSeq());
            catchUpOnGap(context, reference, peerRequests, subscriptions, publications, store);
        }
    }

    private static void catchUpOnGap(ZContext context, ReferenceClient reference,
                                     ZMQ.Socket peerRequests, ZMQ.Socket subscriptions,
                                     ZMQ.Socket publications, Store store) throws Exception {
        long now = System.nanoTime();
        if (now - lastCatchUpNanos >= CATCH_UP_COOLDOWN_NANOS) {
            catchUp(context, reference, peerRequests, subscriptions, publications, store);
        }
    }

    private static void catchUp(ZContext context, ReferenceClient reference,
                                ZMQ.Socket peerRequests, ZMQ.Socket subscriptions,
                                ZMQ.Socket publications, Store store) throws Exception {
        lastCatchUpNanos = System.nanoTime();
        List<ServerInfo> servers = reference.listServers();
        if (servers == null) {
            System.out.println("reparo adiado: referência indisponível");
            return;
        }
        for (ServerInfo server : servers) {
            if (!server.getName().equals(serverName)) {
                int applied = pullFrom(context, reference, peerRequests, subscriptions,
                        publications, server, store);
                if (applied > 0) {
                    System.out.printf("reparo: %d entradas de '%s' | vetor: %s%n",
                            applied, server.getName(), store.vector());
                }
            }
        }
    }

    private static int pullFrom(ZContext context, ReferenceClient reference,
                                ZMQ.Socket peerRequests, ZMQ.Socket subscriptions,
                                ZMQ.Socket publications, ServerInfo server, Store store)
            throws Exception {
        int totalApplied = 0;
        while (true) {
            SyncRequest sync = SyncRequest.newBuilder()
                    .putAllVector(store.vector())
                    .putAllLogVector(store.logVector())
                    .build();
            PeerCallResult result = callPeer(context, reference, peerRequests, subscriptions,
                    publications, store, server.getAddress(),
                    ServerRequest.newBuilder().setSync(sync).build());
            if (result == null || !result.response().hasSyncReply()) {
                System.out.printf("reparo: '%s' não respondeu%n", server.getName());
                return totalApplied;
            }

            SyncReply reply = result.response().getSyncReply();
            int applied = applySync(reply, store);
            totalApplied += applied;
            boolean pageFull = reply.getEntriesCount() == Store.SYNC_PAGE
                    || reply.getLogEntriesCount() == Store.SYNC_PAGE;
            if (applied == 0 || !pageFull) {
                return totalApplied;
            }
        }
    }

    private static int applySync(SyncReply reply, Store store) throws SQLException {
        int applied = 0;
        for (ReplicaEntry entry : reply.getEntriesList()) {
            TimeUtil.observeLogicalClock(entry.getClock());
            if (store.apply(entry) == Store.ApplyResult.APPLIED) {
                applied++;
            }
        }
        for (LogEntry entry : reply.getLogEntriesList()) {
            TimeUtil.observeLogicalClock(entry.getClock());
            if (store.applyLog(entry) == Store.ApplyResult.APPLIED) {
                applied++;
            }
        }
        return applied;
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
                                        ZMQ.Socket peerRequests, ZMQ.Socket subscriptions,
                                        ZMQ.Socket publications, Store store) throws Exception {
        if (serverName.equals(coordinator)) {
            return;
        }
        if (coordinator == null) {
            startElection(context, reference, peerRequests, subscriptions, publications, store);
            return;
        }

        List<ServerInfo> servers = reference.listServers();
        if (servers == null) {
            return;
        }
        String address = servers.stream()
                .filter(server -> server.getName().equals(coordinator))
                .map(ServerInfo::getAddress)
                .findFirst()
                .orElse(null);
        PeerCallResult result = address == null ? null : callPeer(context, reference,
                peerRequests, subscriptions, publications, store, address,
                ServerRequest.newBuilder().setClockRequest(ClockRequest.getDefaultInstance()).build());
        if (result == null) {
            startElection(context, reference, peerRequests, subscriptions, publications, store);
            return;
        }

        TimeUtil.synchronizePhysicalClock(result.response().getTimestamp(),
                result.sentAt(), result.receivedAt());
        System.out.printf("relógio sincronizado com '%s' | ajuste: %+.3f ms%n",
                coordinator, TimeUtil.physicalOffsetMillis());
    }

    private static void startElection(ZContext context, ReferenceClient reference,
                                      ZMQ.Socket peerRequests, ZMQ.Socket subscriptions,
                                      ZMQ.Socket publications, Store store) throws Exception {
        System.out.println("iniciando eleição");
        List<ServerInfo> servers = reference.listServers();
        if (servers == null) {
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
            PeerCallResult result = callPeer(context, reference, peerRequests, subscriptions,
                    publications, store, server.getAddress(),
                    ServerRequest.newBuilder().setElection(election).build());
            if (result != null && result.response().hasElectionReply()
                    && result.response().getElectionReply().getStatus().equals("OK")) {
                coordinator = null;
                System.out.printf("'%s' respondeu à eleição: aguardando anúncio%n", server.getName());
                return;
            }
        }
        announceCoordinator(publications);
    }

    private static PeerCallResult callPeer(ZContext context, ReferenceClient reference,
                                           ZMQ.Socket peerRequests, ZMQ.Socket subscriptions,
                                           ZMQ.Socket publications, Store store, String address,
                                           ServerRequest request) throws Exception {
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

            ZMQ.Poller peerPoller = context.createPoller(2);
            peerPoller.register(peer, ZMQ.Poller.POLLIN);
            peerPoller.register(peerRequests, ZMQ.Poller.POLLIN);
            long deadline = System.nanoTime() + PEER_TIMEOUT_MILLIS * 1_000_000;
            while (true) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    return null;
                }
                long waitMillis = Math.max(1, (remainingNanos + 999_999) / 1_000_000);
                if (peerPoller.poll(waitMillis) <= 0) {
                    continue;
                }
                if (peerPoller.pollin(1)) {
                    electionPending = servePeerRequest(peerRequests, store);
                }
                if (peerPoller.pollin(0)) {
                    ServerResponse response = ServerResponse.parseFrom(peer.recv(0));
                    Instant receivedAt = Instant.now();
                    TimeUtil.observeLogicalClock(response.getClock());
                    return new PeerCallResult(response, sentAt, receivedAt);
                }
            }
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
        sendTopic(publications, "servers", announcement.toByteArray());
        System.out.printf("eleito coordenador | relógio lógico: %d%n",
                TimeUtil.currentLogicalClock());
    }

    private static boolean servePeerRequest(ZMQ.Socket peerRequests, Store store) throws Exception {
        ServerRequest request = ServerRequest.parseFrom(peerRequests.recv(0));
        TimeUtil.observeLogicalClock(request.getClock());
        System.out.printf("servidor: recebido '%s' | relógio lógico: %d%n",
                request.getPayloadCase(), TimeUtil.currentLogicalClock());

        ServerResponse.Builder response = ServerResponse.newBuilder();
        if (request.hasClockRequest()) {
            response.setClockReply(ClockReply.getDefaultInstance());
        } else if (request.hasElection()) {
            response.setElectionReply(ElectionReply.newBuilder().setStatus("OK"));
        } else if (request.hasSync()) {
            SyncRequest sync = request.getSync();
            response.setSyncReply(sistema.proto.SyncReply.newBuilder()
                    .addAllEntries(store.entriesSince(sync.getVectorMap()))
                    .addAllLogEntries(store.logEntriesSince(sync.getLogVectorMap())));
        }
        response.setTimestamp(TimeUtil.now()).setClock(TimeUtil.incrementLogicalClock());
        peerRequests.send(response.build().toByteArray(), 0);
        return request.hasElection();
    }

    private static void handleAnnouncement(byte[] payload) throws Exception {
        CoordinatorAnnouncement announcement = CoordinatorAnnouncement.parseFrom(payload);
        TimeUtil.observeLogicalClock(announcement.getClock());
        coordinator = announcement.getName();
        System.out.printf("coordenador: %s | relógio lógico: %d%n",
                coordinator, TimeUtil.currentLogicalClock());
    }

    private record PeerCallResult(ServerResponse response, Instant sentAt, Instant receivedAt) {}
}
