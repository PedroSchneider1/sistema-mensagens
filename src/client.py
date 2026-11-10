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
    request.timestamp.CopyFrom(now())
    request.clock = tick()
    req.send(request.SerializeToString())
    resp = pb.Response()
    resp.ParseFromString(req.recv())
    observe(resp.clock)
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
    return resp.publish_reply.status


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
            pump(max(0.0, INTERVAL_SECONDS - (time.monotonic() - start)))


if __name__ == "__main__":
    main()
