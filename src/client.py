import zmq
from time import sleep

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://broker:5555")

FLAG_LOGIN          = b"login_"
FLAG_CREATE_CHANNEL = b"create_"
FLAG_LIST_CHANNEL   = b"list_"

def login(user: str) -> bytes:
    socket.send(bytearray(FLAG_LOGIN + user.encode()))
    response = socket.recv()
    return response

def create_channel(channel_name: str) -> bytes:
    socket.send(bytearray(FLAG_CREATE_CHANNEL + channel_name.encode()))
    response = socket.recv()
    return response

# testa um login válido e um inválido
users = ["a", "pedro_client", "gabriel_client"]

for u in users:
    res = login(u)
    if res == b"401":
        print("erro de autenticacao")
    if res == b"200":
        print("logado!!!!")
        # create_channel("canal_pedro")

socket.close()
