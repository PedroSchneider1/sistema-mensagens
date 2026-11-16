import os
import random
import secrets
import time
from datetime import timezone

import zmq
from google.protobuf.timestamp_pb2 import Timestamp
import message_pb2 as pb

BOT_USER = os.environ["BOT_USER"]

MIN_CHANNELS = 5
MIN_SUBSCRIPTIONS = 3
MESSAGES_PER_ROUND = 10
INTERVAL_SECONDS = 1.0
RETRY_SECONDS = 0.5   # espera antes de repetir uma requisição recusada com 503

WORDS = ["olá", "tudo", "bem", "aqui", "canal", "teste", "mensagem",
         "ok", "hoje", "dados", "bot", "sistema"]

context = zmq.Context()

# Requisições ao servidor (via broker)
req = context.socket(zmq.REQ)
req.connect("tcp://broker:5555")

# Recebimento das publicações (via proxy XPUB)
sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")

subscriptions: set[str] = set()

# Relógio lógico do processo
clock = 0

# Vetor de versões da sessão: maior seq que já vimos (lemos ou escrevemos) de cada
# servidor. Vai em toda requisição e o servidor só responde depois de aplicá-lo,
# o que garante leitura monotônica, leitura do que escreveu, escrita monotônica
# e escritas que seguem leituras, mesmo com o broker alternando os servidores.
vector: dict[str, int] = {}

# Para conferir as garantias: tamanho do último histórico visto e quantas
# mensagens publicamos, por canal
history_seen: dict[str, int] = {}
published: dict[str, int] = {}


def now() -> Timestamp:
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


def tick() -> int:
    """Incrementa o relógio lógico antes de enviar uma mensagem."""
    global clock
    clock += 1
    return clock


def observe(received: int) -> None:
    """Atualiza o relógio lógico ao receber uma mensagem."""
    global clock
    clock = max(clock, received)


def fmt(ts: Timestamp) -> str:
    return ts.ToDatetime().replace(tzinfo=timezone.utc).astimezone().strftime("%H:%M:%S.%f")[:-3]


def call(request: pb.Request) -> pb.Response:
    while True:
        request.timestamp.CopyFrom(now())
        request.clock = tick()
        request.vector.update(vector)
        req.send(request.SerializeToString())
        resp = pb.Response()
        resp.ParseFromString(req.recv())
        observe(resp.clock)

        if resp.WhichOneof("payload") == "error" and resp.error.status == 503:
            # Réplica ainda sem o que já vimos: tenta de novo (o broker escolhe outro servidor)
            print("réplica atrasada (503): repetindo a requisição", flush=True)
            time.sleep(RETRY_SECONDS)
            continue

        for origin, seq in resp.vector.items():
            vector[origin] = max(vector.get(origin, 0), seq)
        return resp


def login(user: str) -> int:
    resp = call(pb.Request(login=pb.LoginRequest(user=user, timestamp=now())))
    return resp.login_reply.status


def create_channel(name: str) -> int:
    resp = call(pb.Request(create_channel=pb.CreateChannelRequest(name=name, timestamp=now())))
    return resp.create_channel_reply.status


def list_channels() -> list[str]:
    resp = call(pb.Request(list_channels=pb.ListChannelsRequest(timestamp=now())))
    return list(resp.list_channels_reply.channels)


def publish(channel: str, text: str) -> int:
    resp = call(pb.Request(publish=pb.PublishRequest(
        user=BOT_USER, channel=channel, message=text, timestamp=now())))
    if resp.publish_reply.status == 200:
        published[channel] = published.get(channel, 0) + 1
    return resp.publish_reply.status


def get_history(channel: str) -> list[pb.HistoryEntry] | None:
    resp = call(pb.Request(get_history=pb.GetHistoryRequest(channel=channel, timestamp=now())))
    if resp.get_history_reply.status != 200:
        return None
    return list(resp.get_history_reply.messages)


def check_history(channel: str) -> None:
    """Lê o histórico e confere as garantias de sessão, avisando se alguma falhar."""
    messages = get_history(channel)
    if messages is None:
        print(f"AVISO: histórico de '{channel}' indisponível (404)", flush=True)
        return

    # Leitura monotônica: o histórico nunca encolhe
    if len(messages) < history_seen.get(channel, 0):
        print(f"AVISO: histórico de '{channel}' encolheu: "
              f"{history_seen[channel]} -> {len(messages)}", flush=True)
    history_seen[channel] = max(history_seen.get(channel, 0), len(messages))

    # Leitura do que escreveu: as nossas publicações estão no histórico
    mine = sum(m.sender == BOT_USER for m in messages)
    if mine < published.get(channel, 0):
        print(f"AVISO: histórico de '{channel}' sem as nossas publicações: "
              f"{mine} de {published[channel]}", flush=True)

    # Ordem total igual em todas as réplicas: (relógio lógico, origem, seq)
    order = [(m.clock, m.origin, m.seq) for m in messages]
    if order != sorted(order):
        print(f"AVISO: histórico de '{channel}' fora de ordem", flush=True)


def random_channel_name() -> str:
    return f"canal_{secrets.token_hex(3)}"


def random_message() -> str:
    return " ".join(random.choices(WORDS, k=random.randint(3, 6)))


def setup_channels() -> list[str]:
    channels = list_channels()
    while len(channels) < MIN_CHANNELS:
        name = random_channel_name()
        status = create_channel(name)
        print(f"criar canal '{name}': status {status}", flush=True)
        channels = list_channels()
    return channels


def setup_subscriptions(channels: list[str]) -> None:
    while len(subscriptions) < MIN_SUBSCRIPTIONS:
        available = [c for c in channels if c not in subscriptions]
        if not available:
            break
        channel = random.choice(available)
        sub.setsockopt_string(zmq.SUBSCRIBE, channel)
        subscriptions.add(channel)
        print(f"inscrito no canal '{channel}'", flush=True)


def handle_incoming(topic: bytes, payload: bytes) -> None:
    channel = topic.decode()
    event = pb.Publication()
    event.ParseFromString(payload)
    observe(event.clock)

    # O ZeroMQ casa por prefixo: a inscrição 'canal_1' também recebe 'canal_12'.
    # Filtra o nome exato para não exibir mensagens de outro canal.
    if event.channel != channel:
        return

    print(
        f"[{event.channel}] {event.sender}: {event.message} | "
        f"enviado: {fmt(event.timestamp)} | recebido: {fmt(now())} | "
        f"relógio lógico: {event.clock} -> {clock}",
        flush=True,
    )


def pump(seconds: float) -> None:
    """Recebe e exibe publicações durante `seconds` segundos."""
    deadline = time.monotonic() + seconds
    while (remaining := deadline - time.monotonic()) > 0:
        if sub.poll(int(remaining * 1000)):
            topic, payload = sub.recv_multipart()
            handle_incoming(topic, payload)


def main() -> None:
    status = login(BOT_USER)
    if status != 200:
        print(f"falha no login de '{BOT_USER}' (status {status})", flush=True)
        return
    print(f"logado como '{BOT_USER}'", flush=True)

    channels = setup_channels()
    setup_subscriptions(channels)

    while True:
        channels = list_channels()
        channel = random.choice(channels)
        print(f"--- enviando {MESSAGES_PER_ROUND} mensagens para '{channel}'", flush=True)

        for _ in range(MESSAGES_PER_ROUND):
            start = time.monotonic()
            status = publish(channel, random_message())
            if status != 200:
                print(f"erro ao publicar (status {status})", flush=True)
            check_history(channel)
            pump(max(0.0, INTERVAL_SECONDS - (time.monotonic() - start)))

        print(f"histórico de '{channel}': {history_seen.get(channel, 0)} mensagens "
              f"| vetor: {vector}", flush=True)


if __name__ == "__main__":
    main()
