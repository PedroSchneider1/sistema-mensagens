package sistema;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import sistema.proto.CreateChannelRequest;
import sistema.proto.GetHistoryRequest;
import sistema.proto.HistoryEntry;
import sistema.proto.ListChannelsRequest;
import sistema.proto.LoginRequest;
import sistema.proto.Publication;
import sistema.proto.PublishRequest;
import sistema.proto.Request;
import sistema.proto.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/** Bot behavior for login, channel setup, subscriptions, and publishing. */
final class Client {
    private static final int MIN_CHANNELS = 5;
    private static final int MIN_SUBSCRIPTIONS = 3;
    private static final int MESSAGES_PER_ROUND = 10;
    private static final long MESSAGE_INTERVAL_MILLIS = 1000;
    private static final String[] WORDS = {
            "olá", "tudo", "bem", "aqui", "canal", "teste", "mensagem",
            "ok", "hoje", "dados", "bot", "sistema"
    };
    private static final Map<String, Long> sessionVector = new HashMap<>();
    private static final Map<String, Integer> historySizes = new HashMap<>();
    private static final Map<String, Integer> publishedCounts = new HashMap<>();

    private Client() {}

    static void run() throws Exception {
        String username = System.getenv("BOT_USER");
        if (username == null || username.isBlank()) {
            throw new IllegalArgumentException("BOT_USER não definido");
        }

        try (ZContext context = new ZContext()) {
            ZMQ.Socket requests = context.createSocket(ZMQ.REQ);
            requests.connect("tcp://broker:5555");
            ZMQ.Socket subscriptions = context.createSocket(ZMQ.SUB);
            subscriptions.connect("tcp://proxy:5558");

            login(requests, username);
            List<String> channels = ensureMinimumChannels(requests);
            subscribeToChannels(subscriptions, channels);
            Thread.sleep(500);
            publishContinuously(context, requests, subscriptions, username);
        }
    }

    private static void login(ZMQ.Socket requests, String username) throws Exception {
        while (true) {
            LoginRequest login = LoginRequest.newBuilder()
                    .setUser(username)
                    .setTimestamp(TimeUtil.now())
                    .build();
            Response response = call(requests, Request.newBuilder().setLogin(login).build());
            int status = response.getLoginReply().getStatus();
            if (status == 200) {
                System.out.printf("logado como '%s'%n", username);
                return;
            }

            System.out.printf("falha no login de '%s' (status %d); tentando novamente%n",
                    username, status);
            Thread.sleep(1000);
        }
    }

    private static List<String> ensureMinimumChannels(ZMQ.Socket requests) throws Exception {
        List<String> channels = listChannels(requests);
        while (channels.size() < MIN_CHANNELS) {
            String name = "canal_" + UUID.randomUUID().toString().replace("-", "").substring(0, 6);
            CreateChannelRequest create = CreateChannelRequest.newBuilder()
                    .setName(name)
                    .setTimestamp(TimeUtil.now())
                    .build();
            int status = call(requests, Request.newBuilder().setCreateChannel(create).build())
                    .getCreateChannelReply().getStatus();
            System.out.printf("criar canal '%s': status %d%n", name, status);
            channels = listChannels(requests);
        }
        return channels;
    }

    private static void subscribeToChannels(ZMQ.Socket subscriptions, List<String> channels) {
        Collections.shuffle(channels);
        int count = Math.min(MIN_SUBSCRIPTIONS, channels.size());
        for (int i = 0; i < count; i++) {
            String channel = channels.get(i);
            subscriptions.subscribe(channel);
            System.out.printf("inscrito no canal '%s'%n", channel);
        }
    }

    private static void publishContinuously(ZContext context, ZMQ.Socket requests,
                                            ZMQ.Socket subscriptions, String username)
            throws Exception {
        while (true) {
            List<String> channels = listChannels(requests);
            if (channels.isEmpty()) {
                Thread.sleep(MESSAGE_INTERVAL_MILLIS);
                continue;
            }

            String channel = channels.get(ThreadLocalRandom.current().nextInt(channels.size()));
            System.out.printf("--- enviando %d mensagens para '%s'%n", MESSAGES_PER_ROUND, channel);

            for (int i = 0; i < MESSAGES_PER_ROUND; i++) {
                long startedAt = System.nanoTime();
                PublishRequest publish = PublishRequest.newBuilder()
                        .setUser(username)
                        .setChannel(channel)
                        .setMessage(randomMessage())
                        .setTimestamp(TimeUtil.now())
                        .build();
                int status = call(requests, Request.newBuilder().setPublish(publish).build())
                        .getPublishReply().getStatus();
                if (status != 200) {
                    System.out.printf("erro ao publicar (status %d)%n", status);
                } else {
                    publishedCounts.merge(channel, 1, Integer::sum);
                }
                inspectHistory(requests, channel, username);

                long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000;
                pump(context, subscriptions, Math.max(0, MESSAGE_INTERVAL_MILLIS - elapsedMillis));
            }
        }
    }

    private static Response call(ZMQ.Socket requests, Request request) throws Exception {
        while (true) {
            Request stamped = request.toBuilder().setTimestamp(TimeUtil.now())
                    .setClock(TimeUtil.incrementLogicalClock()).clearVector()
                    .putAllVector(sessionVector).build();
            requests.send(stamped.toByteArray(), 0);
            Response response = Response.parseFrom(requests.recv(0));
            TimeUtil.observeLogicalClock(response.getClock());
            if (response.hasError() && response.getError().getStatus() == 503) {
                System.out.println("servidor atrasado; repetindo requisição após 500 ms");
                Thread.sleep(500);
                continue;
            }
            response.getVectorMap().forEach((origin, seq) ->
                    sessionVector.merge(origin, seq, Math::max));
            return response;
        }
    }

    private static void inspectHistory(ZMQ.Socket requests, String channel, String username) throws Exception {
        GetHistoryRequest historyRequest = GetHistoryRequest.newBuilder().setChannel(channel).build();
        Response response = call(requests,
                Request.newBuilder().setGetHistory(historyRequest).build());
        if (!response.hasGetHistoryReply() || response.getGetHistoryReply().getStatus() != 200) {
            System.out.printf("AVISO: histórico de '%s' indisponível (404)%n", channel);
            return;
        }
        List<HistoryEntry> entries = response.getGetHistoryReply().getMessagesList();
        int previous = historySizes.getOrDefault(channel, 0);
        if (entries.size() < previous) {
            System.out.printf("AVISO: histórico de '%s' encolheu: %d -> %d%n",
                    channel, previous, entries.size());
        }
        historySizes.put(channel, Math.max(previous, entries.size()));
        long ownMessages = entries.stream().filter(entry -> entry.getSender().equals(username))
                .count();
        int expectedOwnMessages = publishedCounts.getOrDefault(channel, 0);
        if (ownMessages < expectedOwnMessages) {
            System.out.printf("AVISO: histórico de '%s' tem %d das %d mensagens publicadas nesta sessão%n",
                    channel, ownMessages, expectedOwnMessages);
        }
        System.out.printf("histórico '%s': %d mensagens | vetor da sessão: %s%n",
                channel, entries.size(), sessionVector);
    }

    private static List<String> listChannels(ZMQ.Socket requests) throws Exception {
        ListChannelsRequest list = ListChannelsRequest.newBuilder()
                .setTimestamp(TimeUtil.now())
                .build();
        Response response = call(requests, Request.newBuilder().setListChannels(list).build());
        return new ArrayList<>(response.getListChannelsReply().getChannelsList());
    }

    private static String randomMessage() {
        int wordCount = ThreadLocalRandom.current().nextInt(3, 7);
        StringJoiner message = new StringJoiner(" ");
        for (int i = 0; i < wordCount; i++) {
            message.add(WORDS[ThreadLocalRandom.current().nextInt(WORDS.length)]);
        }
        return message.toString();
    }

    private static void pump(ZContext context, ZMQ.Socket subscriptions, long waitMillis)
            throws Exception {
        long deadline = System.nanoTime() + waitMillis * 1_000_000;
        ZMQ.Poller poller = context.createPoller(1);
        poller.register(subscriptions, ZMQ.Poller.POLLIN);

        while (System.nanoTime() < deadline) {
            long remainingMillis = Math.max(1, (deadline - System.nanoTime()) / 1_000_000);
            if (poller.poll(remainingMillis) > 0 && poller.pollin(0)) {
                receivePublication(subscriptions);
            }
        }
    }

    private static void receivePublication(ZMQ.Socket subscriptions) throws Exception {
        String topic = subscriptions.recvStr(0);
        Publication publication = Publication.parseFrom(subscriptions.recv(0));
        TimeUtil.observeLogicalClock(publication.getClock());
        if (!publication.getChannel().equals(topic)) {
            return;
        }

        System.out.printf("[%s] %s: %s | enviado: %s | recebido: %s | relógio lógico: %d -> %d%n",
                topic, publication.getSender(), publication.getMessage(),
                TimeUtil.format(publication.getTimestamp()), TimeUtil.format(java.time.Instant.now()),
                publication.getClock(), TimeUtil.currentLogicalClock());
    }
}
