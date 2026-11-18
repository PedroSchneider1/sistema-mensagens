import os
import socket
import time
from datetime import timezone

import zmq
from google.protobuf.timestamp_pb2 import Timestamp
import message_pb2 as pb
from store import Store, APPLIED, GAP

USERS = {"pedro_client", "gabriel_client", "pedro_java", "gabriel_java"}

SERVER_NAME = os.environ.get("SERVER_NAME", socket.gethostname())
DB_PATH = os.environ.get("DB_PATH", f"/data/{SERVER_NAME}.db")
PEER_PORT = int(os.environ.get("PEER_PORT", "5560"))
# Endereço informado à referência para os outros servidores nos contactarem
SERVER_ADDRESS = os.environ.get("SERVER_ADDRESS", f"tcp://{SERVER_NAME}:{PEER_PORT}")
HEARTBEAT_EVERY = 10          # mensagens de clientes entre heartbeats
SYNC_EVERY = 15               # mensagens de clientes entre sincronizações do relógio
REFERENCE_TIMEOUT_MS = 2000
PEER_TIMEOUT_MS = 3000
SESSION_WAIT_MS = 3000        # quanto esperar a replicação de um vetor de cliente antes do 503
SYNC_PAGE = 500               # máximo de entradas (de cada tipo) por resposta de sincronização
CATCH_UP_COOLDOWN = 1.0       # segundos mínimos entre reparos disparados por lacunas

# Relógio lógico do processo
clock = 0

# Ajuste do relógio físico em relação ao coordenador (ns)
offset_ns = 0

# Nome do servidor eleito coordenador (None enquanto não há um)
coordinator: str | None = None

# Um servidor de rank menor pediu eleição: devemos iniciar a nossa
election_pending = False

# Instante (monotônico) do último reparo, para não repeti-lo a cada lacuna
last_catch_up = 0.0


os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
store = Store(DB_PATH, SERVER_NAME)
print(f"armazenamento em {DB_PATH} | vetor: {store.vector}", flush=True)


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


def log_message(direction: str, msg) -> None:
    """Registra a mensagem trocada com o cliente (Request "in" ou Response "out")."""
    kind = msg.WhichOneof("payload") or "empty"
    message_ts = msg.timestamp if msg.HasField("timestamp") else None
    entry = store.append_log(direction, kind, msg.SerializeToString(), message_ts, tick())
    pub.send_multipart([b"eventlog", entry.SerializeToString()])


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

# Anúncios do coordenador e replicação dos outros servidores (via proxy XPUB)
sub = context.socket(zmq.SUB)
sub.connect("tcp://proxy:5558")
sub.setsockopt_string(zmq.SUBSCRIBE, "servers")
sub.setsockopt_string(zmq.SUBSCRIBE, "replica")
sub.setsockopt_string(zmq.SUBSCRIBE, "eventlog")


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

        # Enquanto espera, atende os pedidos dos outros servidores: se dois servidores
        # chamam um ao outro ao mesmo tempo, nenhum deles fica bloqueado até o timeout
        waiting = zmq.Poller()
        waiting.register(sock, zmq.POLLIN)
        waiting.register(peer_rep, zmq.POLLIN)
        deadline = time.monotonic() + PEER_TIMEOUT_MS / 1000
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            events = dict(waiting.poll(int(remaining * 1000) + 1))
            if events.get(peer_rep) == zmq.POLLIN:
                serve_peer()
            if events.get(sock) == zmq.POLLIN:
                break

        resp = pb.ServerResponse()
        resp.ParseFromString(sock.recv())
        received_ns = time.time_ns()
        observe(resp.clock)
        return resp, sent_ns, received_ns
    finally:
        sock.close()


def apply_sync(reply: pb.SyncReply) -> int:
    """Aplica as entradas recebidas de um par. Retorna quantas eram novas.

    Aceita também as entradas da nossa própria origem: é assim que recuperamos
    o nosso seq depois de reiniciar com o volume vazio.
    """
    applied = 0
    for entry in reply.entries:
        observe(entry.clock)
        applied += store.apply(entry) == APPLIED
    for entry in reply.log_entries:
        observe(entry.clock)
        applied += store.apply_log(entry) == APPLIED
    return applied


def pull_from(server: pb.ServerInfo) -> int:
    """Pede a um par tudo o que falta, página por página. Retorna quantas entradas aplicou."""
    total = 0
    while True:
        request = pb.ServerRequest(
            sync=pb.SyncRequest(vector=store.vector, log_vector=store.log_vector))
        result = call_peer(server.address, request)
        if result is None:
            print(f"reparo: '{server.name}' não respondeu", flush=True)
            return total

        reply = result[0].sync_reply
        applied = apply_sync(reply)
        total += applied
        full = SYNC_PAGE in (len(reply.entries), len(reply.log_entries))
        if applied == 0 or not full:
            return total


def catch_up() -> None:
    """Reparo: busca nos outros servidores o que perdemos (reinício ou mensagem perdida)."""
    global last_catch_up

    last_catch_up = time.monotonic()
    servers = list_servers()
    if servers is None:
        print("reparo adiado: referência indisponível", flush=True)
        return

    for server in servers:
        if server.name != SERVER_NAME:
            applied = pull_from(server)
            if applied:
                print(
                    f"reparo: {applied} entradas de '{server.name}' "
                    f"| vetor: {store.vector}",
                    flush=True,
                )


def catch_up_on_gap() -> None:
    """Uma lacuna indica mensagem perdida; repara, mas no máximo uma vez por segundo."""
    if time.monotonic() - last_catch_up >= CATCH_UP_COOLDOWN:
        catch_up()


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


def replicate(entry: pb.ReplicaEntry) -> None:
    """Envia a entrada aos outros servidores (tópico 'replica')."""
    pub.send_multipart([b"replica", entry.SerializeToString()])


def handle_create_channel(name: str) -> pb.Response:
    if store.channel_exists(name):
        status = 409
    else:
        replicate(store.append_channel(name, tick(), now()))
        status = 200
    return pb.Response(
        create_channel_reply=pb.CreateChannelReply(status=status, timestamp=now())
    )


def handle_list_channels() -> pb.Response:
    return pb.Response(
        list_channels_reply=pb.ListChannelsReply(
            channels=store.channels(), timestamp=now()
        )
    )


def handle_get_history(channel: str) -> pb.Response:
    if not store.channel_exists(channel):
        return pb.Response(get_history_reply=pb.GetHistoryReply(status=404, timestamp=now()))

    return pb.Response(
        get_history_reply=pb.GetHistoryReply(
            status=200, messages=store.history(channel), timestamp=now()
        )
    )


def handle_publish(p: pb.PublishRequest) -> pb.Response:
    if not store.channel_exists(p.channel):
        return pb.Response(publish_reply=pb.PublishReply(status=404, timestamp=now()))

    sent_at = now()

    # Persiste a publicação
    replicate(store.append_publication(p.channel, p.user, p.message, tick(), sent_at))

    # Publica: primeiro frame é o tópico (nome do canal), segundo é o protobuf
    event = pb.Publication(
        channel=p.channel, sender=p.user, message=p.message, timestamp=sent_at,
        clock=tick(),
    )
    pub.send_multipart([p.channel.encode(), event.SerializeToString()])

    return pb.Response(publish_reply=pb.PublishReply(status=200, timestamp=now()))


def dispatch(req: pb.Request) -> pb.Response:
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

    elif kind == "get_history":
        resp = handle_get_history(req.get_history.channel)

    elif kind == "publish":
        resp = handle_publish(req.publish)

    else:
        print(f"Mensagem desconhecida: {kind}", flush=True)
        resp = pb.Response(
            error=pb.ErrorReply(error=f"unknown command: {kind}", timestamp=now())
        )

    return resp


def wait_for_vector(vector) -> bool:
    """Espera esta réplica aplicar tudo o que o cliente já viu (leu ou escreveu).

    Enquanto espera, continua tratando as publicações dos outros servidores e as
    requisições deles. Retorna False se o vetor não chegou a tempo.
    """
    if store.covers(vector):
        return True

    print(f"aguardando réplica: cliente viu {dict(vector)}, temos {store.vector}", flush=True)
    waiting = zmq.Poller()
    waiting.register(sub, zmq.POLLIN)
    waiting.register(peer_rep, zmq.POLLIN)

    deadline = time.monotonic() + SESSION_WAIT_MS / 1000
    while not store.covers(vector):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        events = dict(waiting.poll(int(remaining * 1000) + 1))
        if events.get(sub) == zmq.POLLIN:
            serve_sub()
        if events.get(peer_rep) == zmq.POLLIN:
            serve_peer()
    return True


def handle(req: pb.Request) -> pb.Response:
    """Garantias de sessão: só atende depois de aplicar o vetor do cliente."""
    if wait_for_vector(req.vector):
        resp = dispatch(req)
        # Vetor aplicado, já com a escrita desta requisição: o cliente o incorpora ao seu
        resp.vector.update(store.vector)
    else:
        print("réplica atrasada: respondendo 503", flush=True)
        resp = pb.Response(
            error=pb.ErrorReply(error="replica behind", status=503, timestamp=now())
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

    log_message("in", req)
    resp = handle(req)
    log_message("out", resp)

    rep.send(resp.SerializeToString())

    received += 1
    if received % HEARTBEAT_EVERY == 0:
        send_heartbeat()
        # Também repara periodicamente: uma última entrada perdida não gera lacuna
        catch_up()
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
    elif kind == "sync":
        # Cada lista sai truncada na página; como as entradas vêm por origem e em
        # ordem de seq, o que for enviado continua sem lacunas para quem aplica
        resp.sync_reply.entries.extend(store.entries_since(req.sync.vector)[:SYNC_PAGE])
        resp.sync_reply.log_entries.extend(
            store.log_entries_since(req.sync.log_vector)[:SYNC_PAGE])
    resp.timestamp.CopyFrom(now())
    resp.clock = tick()
    peer_rep.send(resp.SerializeToString())

    if kind == "election":
        # Responde primeiro e só depois inicia a própria eleição, para o
        # servidor que pediu não esperar por ela
        election_pending = True


def handle_announcement(payload: bytes) -> None:
    """Registra o coordenador anunciado no tópico 'servers'."""
    global coordinator

    event = pb.CoordinatorAnnouncement()
    event.ParseFromString(payload)
    observe(event.clock)
    coordinator = event.name
    print(f"coordenador: {coordinator} | relógio lógico: {clock}", flush=True)


def apply_replica(payload: bytes) -> None:
    """Aplica uma entrada de estado publicada por outro servidor (tópico 'replica')."""
    entry = pb.ReplicaEntry()
    entry.ParseFromString(payload)
    if entry.origin == SERVER_NAME:
        return
    observe(entry.clock)

    result = store.apply(entry)
    if result == APPLIED:
        print(
            f"replicado: {entry.WhichOneof('payload')} {entry.origin}#{entry.seq} "
            f"| vetor: {store.vector}",
            flush=True,
        )
    elif result == GAP:
        print(
            f"lacuna em '{entry.origin}': esperado #{store.vector.get(entry.origin, 0) + 1}, "
            f"recebido #{entry.seq}",
            flush=True,
        )
        catch_up_on_gap()


def apply_log(payload: bytes) -> None:
    """Aplica um registro de mensagem de outro servidor (tópico 'eventlog')."""
    entry = pb.LogEntry()
    entry.ParseFromString(payload)
    if entry.origin == SERVER_NAME:
        return
    observe(entry.clock)

    if store.apply_log(entry) == GAP:
        print(
            f"lacuna no log de '{entry.origin}': "
            f"esperado #{store.log_vector.get(entry.origin, 0) + 1}, recebido #{entry.seq}",
            flush=True,
        )
        catch_up_on_gap()


def serve_sub() -> None:
    """Trata uma mensagem recebida pelo SUB, conforme o tópico."""
    topic, payload = sub.recv_multipart()
    if topic == b"servers":
        handle_announcement(payload)
    elif topic == b"replica":
        apply_replica(payload)
    elif topic == b"eventlog":
        apply_log(payload)


received = 0

# Ao iniciar (ou reiniciar), busca o que os outros servidores já têm antes de atender
catch_up()
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
        serve_sub()

    if election_pending:
        election_pending = False
        start_election()
