import zmq
from google.protobuf.timestamp_pb2 import Timestamp
import message_pb2 as pb

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
    socket.send(handle(req).SerializeToString())