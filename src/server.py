import os
import socket
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

ALTER TABLE messages ADD COLUMN IF NOT EXISTS clock BIGINT;

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

SERVER_NAME = os.environ.get("SERVER_NAME", socket.gethostname())
PEER_PORT = int(os.environ.get("PEER_PORT", "5560"))
# Endereço informado à referência para os outros servidores nos contactarem
SERVER_ADDRESS = os.environ.get("SERVER_ADDRESS", f"tcp://{SERVER_NAME}:{PEER_PORT}")
HEARTBEAT_EVERY = 10          # mensagens de clientes entre heartbeats
SYNC_EVERY = 15               # mensagens de clientes entre sincronizações do relógio
REFERENCE_TIMEOUT_MS = 2000
PEER_TIMEOUT_MS = 3000

# Relógio lógico do processo
clock = 0

# Ajuste do relógio físico em relação ao coordenador (ns)
offset_ns = 0

# Nome do servidor eleito coordenador (None enquanto não há um)
coordinator: str | None = None

# Um servidor de rank menor pediu eleição: devemos iniciar a nossa
election_pending = False


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
    ts.FromNanoseconds(time.time_ns() + offset_ns)
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


def to_dt(ts: Timestamp):
    return ts.ToDatetime().replace(tzinfo=timezone.utc)


def save_message(direction: str, msg) -> None:
    kind = msg.WhichOneof("payload")
    payload = MessageToDict(getattr(msg, kind)) if kind else {}
    ts = to_dt(msg.timestamp) if msg.HasField("timestamp") else None

    db.execute(
        "INSERT INTO messages (direction, kind, payload, message_ts, clock) "
        "VALUES (%s, %s, %s, %s, %s)",
        (direction, kind or "empty", Jsonb(payload), ts, msg.clock),
    )


context = zmq.Context()

# Atendimento das requisições do bot (via broker)
rep = context.socket(zmq.REP)
rep.connect("tcp://broker:5556")

# Publicação nos canais (via proxy XSUB)
pub = context.socket(zmq.PUB)
pub.connect("tcp://proxy:5557")

# Requisições de outros servidores (relógio e eleição)
peer_rep = context.socket(zmq.REP)
peer_rep.bind(f"tcp://*:{PEER_PORT}")

# Anúncios do coordenador eleito (via proxy XPUB)
sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")
sub.setsockopt_string(zmq.SUBSCRIBE, "servers")


def connect_reference() -> zmq.Socket:
    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect("tcp://reference:5559")
    return sock


# Serviço de referência (rank, lista de servidores e heartbeat)
ref = connect_reference()


def call_reference(request: pb.ReferenceRequest) -> pb.ReferenceResponse | None:
    """Envia a requisição à referência.

    Retorna None se a referência não responder a tempo.
    """
    global ref

    request.clock = tick()
    request.timestamp.CopyFrom(now())
    ref.send(request.SerializeToString())

    if not ref.poll(REFERENCE_TIMEOUT_MS):
        # REQ fica preso esperando o reply: descarta o socket e recria
        ref.close()
        ref = connect_reference()
        return None

    resp = pb.ReferenceResponse()
    resp.ParseFromString(ref.recv())
    observe(resp.clock)
    return resp


def request_rank() -> int:
    while True:
        resp = call_reference(pb.ReferenceRequest(
            rank=pb.RankRequest(name=SERVER_NAME, address=SERVER_ADDRESS)))
        if resp is not None:
            return resp.rank_reply.rank
        print("aguardando serviço de referência", flush=True)


def send_heartbeat() -> None:
    global rank
    resp = call_reference(pb.ReferenceRequest(heartbeat=pb.HeartbeatRequest(name=SERVER_NAME)))
    if resp is None:
        print("heartbeat sem resposta da referência", flush=True)
    elif resp.heartbeat_reply.status != 200:
        # A referência nos removeu da lista: registra de novo
        rank = request_rank()
        print(f"registrado novamente com rank {rank}", flush=True)
    else:
        print(f"heartbeat ok | ajuste do relógio: {offset_ns / 1e6:+.3f} ms", flush=True)


def list_servers() -> list[pb.ServerInfo] | None:
    resp = call_reference(pb.ReferenceRequest(list=pb.ServerListRequest()))
    return list(resp.list_reply.servers) if resp is not None else None


def call_peer(address: str, request: pb.ServerRequest) -> tuple[pb.ServerResponse, int, int] | None:
    """Envia a requisição a outro servidor.

    Retorna a resposta com os instantes de envio e recebimento (ns), ou None se
    o servidor não responder a tempo.
    """
    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(address)

    try:
        request.clock = tick()
        request.timestamp.CopyFrom(now())
        sent_ns = time.time_ns()
        sock.send(request.SerializeToString())

        if not sock.poll(PEER_TIMEOUT_MS):
            return None

        resp = pb.ServerResponse()
        resp.ParseFromString(sock.recv())
        received_ns = time.time_ns()
        observe(resp.clock)
        return resp, sent_ns, received_ns
    finally:
        sock.close()


def announce_coordinator() -> None:
    """Assume a coordenação e avisa os outros servidores no tópico 'servers'."""
    global coordinator
    coordinator = SERVER_NAME
    event = pb.CoordinatorAnnouncement(name=SERVER_NAME, timestamp=now(), clock=tick())
    pub.send_multipart([b"servers", event.SerializeToString()])
    print(f"eleito coordenador | relógio lógico: {clock}", flush=True)


def start_election() -> None:
    """Eleição (bully): vence o servidor de maior rank que estiver disponível."""
    global coordinator

    print("iniciando eleição", flush=True)
    servers = list_servers()
    if servers is None:
        # Sem a lista não há como saber quem está disponível: tenta na próxima sincronização
        coordinator = None
        print("eleição adiada: referência indisponível", flush=True)
        return

    for server in servers:
        if server.rank <= rank:
            continue
        request = pb.ServerRequest(
            election=pb.ElectionRequest(name=SERVER_NAME, rank=rank))
        result = call_peer(server.address, request)
        if result is not None and result[0].election_reply.status == "OK":
            # Um servidor de rank maior assume a eleição e vai anunciar o resultado
            coordinator = None
            print(f"'{server.name}' respondeu à eleição: aguardando anúncio", flush=True)
            return

    announce_coordinator()


def sync_clock() -> None:
    """Acerta o relógio físico pela hora do coordenador (Cristian)."""
    global offset_ns

    if coordinator == SERVER_NAME:
        return
    if coordinator is None:
        start_election()
        return

    servers = list_servers()
    address = next((s.address for s in servers or [] if s.name == coordinator), None)
    result = call_peer(address, pb.ServerRequest(clock_request=pb.ClockRequest())) if address else None
    if result is None:
        start_election()
        return

    resp, sent_ns, received_ns = result
    # Hora do coordenador + metade do tempo de ida e volta
    coordinator_ns = resp.timestamp.ToNanoseconds() + (received_ns - sent_ns) // 2
    offset_ns = coordinator_ns - received_ns
    print(f"relógio sincronizado com '{coordinator}' | ajuste: {offset_ns / 1e6:+.3f} ms", flush=True)


rank = request_rank()
print(f"servidor '{SERVER_NAME}' com rank {rank} ({SERVER_ADDRESS})", flush=True)


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
        channel=p.channel, sender=p.user, message=p.message, timestamp=sent_at,
        clock=tick(),
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
    resp.clock = tick()
    return resp


def serve_client() -> None:
    """Atende uma requisição de cliente recebida pelo broker."""
    global received

    req = pb.Request()
    req.ParseFromString(rep.recv())
    observe(req.clock)
    print(
        f"[{to_dt(req.timestamp)}] recebido: {req.WhichOneof('payload')} "
        f"| relógio lógico: {clock}",
        flush=True,
    )

    save_message("in", req)
    resp = handle(req)
    save_message("out", resp)

    rep.send(resp.SerializeToString())

    received += 1
    if received % HEARTBEAT_EVERY == 0:
        send_heartbeat()
    if received % SYNC_EVERY == 0:
        sync_clock()


def serve_peer() -> None:
    """Atende uma requisição de outro servidor."""
    global election_pending

    req = pb.ServerRequest()
    req.ParseFromString(peer_rep.recv())
    observe(req.clock)
    kind = req.WhichOneof("payload")
    print(f"servidor: recebido '{kind}' | relógio lógico: {clock}", flush=True)

    resp = pb.ServerResponse()
    if kind == "clock_request":
        resp.clock_reply.SetInParent()
    elif kind == "election":
        resp.election_reply.status = "OK"
    resp.timestamp.CopyFrom(now())
    resp.clock = tick()
    peer_rep.send(resp.SerializeToString())

    if kind == "election":
        # Responde primeiro e só depois inicia a própria eleição, para o
        # servidor que pediu não esperar por ela
        election_pending = True


def handle_announcement() -> None:
    """Registra o coordenador anunciado no tópico 'servers'."""
    global coordinator

    _topic, payload = sub.recv_multipart()
    event = pb.CoordinatorAnnouncement()
    event.ParseFromString(payload)
    observe(event.clock)
    coordinator = event.name
    print(f"coordenador: {coordinator} | relógio lógico: {clock}", flush=True)


received = 0

start_election()

poller = zmq.Poller()
poller.register(rep, zmq.POLLIN)
poller.register(peer_rep, zmq.POLLIN)
poller.register(sub, zmq.POLLIN)

while True:
    events = dict(poller.poll())

    if events.get(rep) == zmq.POLLIN:
        serve_client()
    if events.get(peer_rep) == zmq.POLLIN:
        serve_peer()
    if events.get(sub) == zmq.POLLIN:
        handle_announcement()

    if election_pending:
        election_pending = False
        start_election()
