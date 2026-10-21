import zmq
import message_pb2 as pb

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:5555")


def call(req: pb.Request) -> pb.Response:
    req.timestamp.GetCurrentTime()
    socket.send(req.SerializeToString())
    resp = pb.Response()
    resp.ParseFromString(socket.recv())
    return resp


def connection_test(text: str) -> str:
    resp = call(pb.Request(conn=pb.ConnRequest(message=text)))
    return resp.conn_reply.message


def login(user: str) -> int:
    resp = call(pb.Request(login=pb.LoginRequest(user=user)))
    return resp.login_reply.status


def create_channel(name: str) -> int:
    resp = call(pb.Request(create_channel=pb.CreateChannelRequest(name=name)))
    return resp.create_channel_reply.status


# testa um login válido e um inválido
users = ["a", "pedro_client", "gabriel_client"]

for u in users:
    status = login(u)
    if status == 401:
        print(f"erro de autenticacao: usuario '{u}'")
    elif status == 200:
        print(f"logado como '{u}'!!!!")

if create_channel("canal_pedro") == 200:
    print("canal criado com sucesso!!!!")

socket.close()