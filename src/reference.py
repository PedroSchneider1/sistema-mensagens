import os
import time

import zmq
from google.protobuf.timestamp_pb2 import Timestamp
import message_pb2 as pb

# Tempo sem heartbeat para o servidor ser removido da lista
HEARTBEAT_TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT", "30"))

context = zmq.Context()

# Atendimento dos servidores (REQ/REP)
rep = context.socket(zmq.REP)
rep.bind("tcp://*:5559")

# Relógio lógico do processo
clock = 0

# Rank atribuído a cada nome (mantido mesmo que o servidor saia da lista)
ranks: dict[str, int] = {}
# Servidores disponíveis: nome -> instante (monotônico) do último sinal de vida
last_seen: dict[str, float] = {}


def now() -> Timestamp:
    ts = Timestamp()
    ts.GetCurrentTime()
    return ts


def remove_expired() -> None:
    limit = time.monotonic() - HEARTBEAT_TIMEOUT
    for name in [n for n, seen in last_seen.items() if seen < limit]:
        del last_seen[name]
        print(f"servidor '{name}' removido (sem heartbeat)", flush=True)


def handle_rank(name: str) -> pb.ReferenceResponse:
    if name not in ranks:
        ranks[name] = len(ranks) + 1
    last_seen[name] = time.monotonic()
    print(f"servidor '{name}' registrado com rank {ranks[name]}", flush=True)
    return pb.ReferenceResponse(rank_reply=pb.RankReply(rank=ranks[name]))


def handle_list() -> pb.ReferenceResponse:
    servers = [pb.ServerInfo(name=n, rank=ranks[n]) for n in sorted(last_seen, key=ranks.get)]
    return pb.ReferenceResponse(list_reply=pb.ServerListReply(servers=servers))


def handle_heartbeat(name: str) -> pb.ReferenceResponse:
    if name not in last_seen:
        # Foi removido por falta de heartbeat: precisa pedir o rank de novo
        status = 404
    else:
        last_seen[name] = time.monotonic()
        status = 200
    return pb.ReferenceResponse(heartbeat_reply=pb.HeartbeatReply(status=status))


def handle(req: pb.ReferenceRequest) -> pb.ReferenceResponse:
    kind = req.WhichOneof("payload")

    if kind == "rank":
        return handle_rank(req.rank.name)
    if kind == "list":
        return handle_list()
    if kind == "heartbeat":
        return handle_heartbeat(req.heartbeat.name)

    print(f"Mensagem desconhecida: {kind}", flush=True)
    return pb.ReferenceResponse()


print(f"referência: REP 5559 (heartbeat timeout {HEARTBEAT_TIMEOUT}s)", flush=True)

while True:
    # Acorda periodicamente para remover servidores mesmo sem requisições
    if not rep.poll(1000):
        remove_expired()
        continue

    req = pb.ReferenceRequest()
    req.ParseFromString(rep.recv())
    remove_expired()

    # Relógio lógico: max(local, recebido) ao receber
    clock = max(clock, req.clock)
    print(f"recebido: {req.WhichOneof('payload')} | relógio lógico: {clock}", flush=True)

    resp = handle(req)

    # Relógio lógico: incrementa antes de enviar
    clock += 1
    resp.clock = clock
    resp.timestamp.CopyFrom(now())
    rep.send(resp.SerializeToString())
