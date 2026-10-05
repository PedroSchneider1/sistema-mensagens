import os
import time
from datetime import timezone

import zmq
import psycopg
from psycopg.types.json import Jsonb
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
import message_pb2 as pb

DSN = (
    f"host={os.environ.get('POSTGRES_HOST', 'postgres')} "
    f"port=5432 "
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']}"
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id          BIGSERIAL PRIMARY KEY,
    direction   TEXT NOT NULL CHECK (direction IN ('in', 'out')),
    kind        TEXT NOT NULL,
    payload     JSONB NOT NULL,
    message_ts  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS channels (
    name        TEXT PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS publications (
    id          BIGSERIAL PRIMARY KEY,
    channel     TEXT NOT NULL REFERENCES channels(name),
    sender      TEXT NOT NULL,
    body        TEXT NOT NULL,
    sent_at     TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

USERS = {"pedro_client", "gabriel_client"}


def connect_db():
    while True:
        try:
            return psycopg.connect(DSN, autocommit=True)
        except psycopg.OperationalError as e:
            print(f"aguardando postgres: {e}", flush=True)
            time.sleep(2)


db = connect_db()
db.execute(SCHEMA)


def now() -> Timestamp:
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


def to_dt(ts: Timestamp):
    return ts.ToDatetime().replace(tzinfo=timezone.utc)


def save_message(direction: str, msg) -> None:
    kind = msg.WhichOneof("payload")
    payload = MessageToDict(getattr(msg, kind)) if kind else {}
    ts = to_dt(msg.timestamp) if msg.HasField("timestamp") else None

    db.execute(
        "INSERT INTO messages (direction, kind, payload, message_ts) "
        "VALUES (%s, %s, %s, %s)",
        (direction, kind or "empty", Jsonb(payload), ts),
    )


context = zmq.Context()

# Atendimento das requisições do bot (via broker)
rep = context.socket(zmq.REP)
rep.connect("tcp://broker:5556")

# Publicação nos canais (via proxy XSUB)
pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")


def handle_create_channel(name: str) -> pb.Response:
    row = db.execute(
        "INSERT INTO channels (name) VALUES (%s) ON CONFLICT DO NOTHING RETURNING name",
        (name,),
    ).fetchone()
    status = 200 if row else 409
    return pb.Response(
        create_channel_reply=pb.CreateChannelReply(status=status, timestamp=now())
    )


def handle_list_channels() -> pb.Response:
    rows = db.execute("SELECT name FROM channels ORDER BY name").fetchall()
    return pb.Response(
        list_channels_reply=pb.ListChannelsReply(
            channels=[r[0] for r in rows], timestamp=now()
        )
    )


def handle_publish(p: pb.PublishRequest) -> pb.Response:
    exists = db.execute("SELECT 1 FROM channels WHERE name = %s", (p.channel,)).fetchone()
    if exists is None:
        return pb.Response(publish_reply=pb.PublishReply(status=404, timestamp=now()))

    sent_at = now()

    # Persiste a publicação
    db.execute(
        "INSERT INTO publications (channel, sender, body, sent_at) VALUES (%s, %s, %s, %s)",
        (p.channel, p.user, p.message, to_dt(sent_at)),
    )

    # Publica: primeiro frame é o tópico (nome do canal), segundo é o protobuf
    event = pb.Publication(
        channel=p.channel, sender=p.user, message=p.message, timestamp=sent_at
    )
    pub.send_multipart([p.channel.encode(), event.SerializeToString()])

    return pb.Response(publish_reply=pb.PublishReply(status=200, timestamp=now()))


def handle(req: pb.Request) -> pb.Response:
    kind = req.WhichOneof("payload")

    if kind == "conn":
        resp = pb.Response(
            conn_reply=pb.ConnReply(message=f"pong: {req.conn.message}", timestamp=now())
        )

    elif kind == "login":
        user = req.login.user
        print(f"Tentativa de login: {user}", flush=True)
        status = 200 if user in USERS else 401
        resp = pb.Response(login_reply=pb.LoginReply(status=status, timestamp=now()))

    elif kind == "create_channel":
        resp = handle_create_channel(req.create_channel.name)

    elif kind == "list_channels":
        resp = handle_list_channels()

    elif kind == "publish":
        resp = handle_publish(req.publish)

    else:
        print(f"Mensagem desconhecida: {kind}", flush=True)
        resp = pb.Response(
            error=pb.ErrorReply(error=f"unknown command: {kind}", timestamp=now())
        )

    resp.timestamp.CopyFrom(now())
    return resp


while True:
    raw = rep.recv()
    req = pb.Request()
    req.ParseFromString(raw)
    print(f"[{to_dt(req.timestamp)}] recebido: {req.WhichOneof('payload')}", flush=True)

    save_message("in", req)
    resp = handle(req)
    save_message("out", resp)

    rep.send(resp.SerializeToString())
