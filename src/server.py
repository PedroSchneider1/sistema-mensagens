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
"""


def connect_db():
    while True:
        try:
            return psycopg.connect(DSN, autocommit=True)
        except psycopg.OperationalError as e:
            print(f"aguardando postgres: {e}", flush=True)
            time.sleep(2)


db = connect_db()
db.execute(SCHEMA)


def save_message(direction: str, msg) -> None:
    kind = msg.WhichOneof("payload")
    if kind is None:
        payload = {}
    else:
        payload = MessageToDict(getattr(msg, kind))

    ts = None
    if msg.HasField("timestamp"):
        ts = msg.timestamp.ToDatetime().replace(tzinfo=timezone.utc)

    db.execute(
        "INSERT INTO messages (direction, kind, payload, message_ts) "
        "VALUES (%s, %s, %s, %s)",
        (direction, kind or "empty", Jsonb(payload), ts),
    )


context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

USERS = {"pedro_client", "gabriel_client"}
channels: list[str] = []


def now() -> Timestamp:
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


def handle(req: pb.Request) -> pb.Response:
    kind = req.WhichOneof("payload")

    if kind == "conn":
        reply = pb.Response(
            conn_reply=pb.ConnReply(message=f"pong: {req.conn.message}", timestamp=now())
        )

    elif kind == "login":
        user = req.login.user
        print(f"Tentativa de login: {user}", flush=True)
        status = 200 if user in USERS else 401
        reply = pb.Response(login_reply=pb.LoginReply(status=status, timestamp=now()))

    elif kind == "create_channel":
        channels.append(req.create_channel.name)
        reply = pb.Response(
            create_channel_reply=pb.CreateChannelReply(status=200, timestamp=now())
        )

    else:
        print(f"Mensagem desconhecida: {kind}", flush=True)
        reply = pb.Response(
            error=pb.ErrorReply(error=f"unknown command: {kind}", timestamp=now())
        )

    reply.timestamp.CopyFrom(now())
    return reply


while True:
    raw = socket.recv()
    req = pb.Request()
    req.ParseFromString(raw)
    print(f"[{req.timestamp.ToDatetime()}] recebido: {req.WhichOneof('payload')}", flush=True)

    save_message("in", req)

    resp = handle(req)
    save_message("out", resp)

    socket.send(resp.SerializeToString())