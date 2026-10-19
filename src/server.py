import zmq
import ast

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

FLAG_LOGIN          = b"login_"
FLAG_CREATE_CHANNEL = b"create_"
FLAG_LIST_CHANNEL   = b"list_"

users = [b"pedro_client", b"gabriel_client"]
channels = []

while True:
    message = socket.recv()
    if message.startswith(FLAG_LOGIN):
        message = message.removeprefix(FLAG_LOGIN)
        print(f"Tentativa de login: {message}", flush=True)
        if message in users:
            socket.send(b"200")
        else:
            socket.send(b"401")
    elif message.startswith(FLAG_CREATE_CHANNEL):
        message = message.removeprefix(FLAG_CREATE_CHANNEL)
        channels.append(message)
        socket.send(b"200")
    else:
        print(f"Mensagem desconhecida: {message}", flush=True)
        socket.send_string("ERRO: Comando desconhecido")

socket.close()
context.close()
