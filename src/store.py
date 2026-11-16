import sqlite3

from google.protobuf.timestamp_pb2 import Timestamp
import message_pb2 as pb

SCHEMA = """
-- Alterações de estado replicadas (canais e publicações). O id é (origin, seq):
-- seq é contado por servidor de origem e não tem lacunas.
CREATE TABLE IF NOT EXISTS entries (
    origin   TEXT    NOT NULL,
    seq      INTEGER NOT NULL,
    kind     TEXT    NOT NULL CHECK (kind IN ('channel', 'publication')),
    channel  TEXT    NOT NULL,
    sender   TEXT,
    body     TEXT,
    clock    INTEGER NOT NULL,
    ts_ns    INTEGER NOT NULL,
    PRIMARY KEY (origin, seq)
);
CREATE INDEX IF NOT EXISTS entries_channel ON entries (channel, clock, origin, seq);

-- Registro das mensagens trocadas com os clientes, com sequência própria por origem.
-- payload é o Request ("in") ou o Response ("out") serializado em protobuf. Para inspecionar:
--   import sqlite3, message_pb2 as pb
--   for d, p in sqlite3.connect("server_1.db").execute("SELECT direction, payload FROM message_log"):
--       print((pb.Request if d == "in" else pb.Response).FromString(p))
CREATE TABLE IF NOT EXISTS message_log (
    origin         TEXT    NOT NULL,
    seq            INTEGER NOT NULL,
    direction      TEXT    NOT NULL CHECK (direction IN ('in', 'out')),
    kind           TEXT    NOT NULL,
    payload        BLOB    NOT NULL,
    message_ts_ns  INTEGER,
    clock          INTEGER NOT NULL,
    PRIMARY KEY (origin, seq)
);
"""

# Resultado de apply(): a entrada foi gravada, já existia ou deixou uma lacuna
APPLIED, DUPLICATE, GAP = "applied", "duplicate", "gap"


def to_ns(ts: Timestamp) -> int:
    return ts.ToNanoseconds()


def to_ts(ns: int) -> Timestamp:
    ts = Timestamp()
    ts.FromNanoseconds(ns)
    return ts


class Store:
    """Banco SQLite de um servidor: guarda as suas entradas e as replicadas dos outros."""

    def __init__(self, path: str, origin: str):
        self.origin = origin
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.executescript(SCHEMA)

        # Maior seq aplicado de cada origem. Como as entradas são aplicadas sem
        # lacunas, o maior seq é também o número de entradas da origem.
        self.vector = self._load_vector("entries")
        self.log_vector = self._load_vector("message_log")

    def _load_vector(self, table: str) -> dict[str, int]:
        rows = self.db.execute(f"SELECT origin, MAX(seq) FROM {table} GROUP BY origin")
        return dict(rows.fetchall())

    # ---------- Estado ----------

    def covers(self, vector) -> bool:
        """Já aplicamos tudo o que o vetor informa?"""
        return all(self.vector.get(origin, 0) >= seq for origin, seq in vector.items())

    def channel_exists(self, name: str) -> bool:
        row = self.db.execute(
            "SELECT 1 FROM entries WHERE kind = 'channel' AND channel = ?", (name,)
        ).fetchone()
        return row is not None

    def channels(self) -> list[str]:
        rows = self.db.execute(
            "SELECT DISTINCT channel FROM entries WHERE kind = 'channel' ORDER BY channel"
        )
        return [r[0] for r in rows]

    def history(self, channel: str) -> list[pb.HistoryEntry]:
        """Mensagens do canal na mesma ordem em todas as réplicas: (clock, origin, seq)."""
        rows = self.db.execute(
            "SELECT origin, seq, sender, body, clock, ts_ns FROM entries "
            "WHERE kind = 'publication' AND channel = ? ORDER BY clock, origin, seq",
            (channel,),
        )
        return [
            pb.HistoryEntry(
                origin=origin, seq=seq, sender=sender, message=body,
                clock=clock, timestamp=to_ts(ts_ns),
            )
            for origin, seq, sender, body, clock, ts_ns in rows
        ]

    def append_channel(self, name: str, clock: int, timestamp: Timestamp) -> pb.ReplicaEntry:
        """Registra um canal criado neste servidor."""
        entry = pb.ReplicaEntry(
            origin=self.origin, seq=self.vector.get(self.origin, 0) + 1,
            clock=clock, timestamp=timestamp, channel=pb.ChannelCreated(name=name),
        )
        self._insert(entry)
        return entry

    def append_publication(
        self, channel: str, sender: str, message: str, clock: int, timestamp: Timestamp
    ) -> pb.ReplicaEntry:
        """Registra uma mensagem publicada através deste servidor."""
        entry = pb.ReplicaEntry(
            origin=self.origin, seq=self.vector.get(self.origin, 0) + 1,
            clock=clock, timestamp=timestamp,
            publication=pb.MessagePublished(channel=channel, sender=sender, message=message),
        )
        self._insert(entry)
        return entry

    def apply(self, entry: pb.ReplicaEntry) -> str:
        """Aplica uma entrada de outro servidor. Só aceita a próxima da sua origem."""
        last = self.vector.get(entry.origin, 0)
        if entry.seq <= last:
            return DUPLICATE
        if entry.seq > last + 1:
            return GAP
        self._insert(entry)
        return APPLIED

    def entries_since(self, vector) -> list[pb.ReplicaEntry]:
        """Entradas que o vetor informado ainda não cobre, por origem e em ordem de seq."""
        entries = []
        for origin in self.vector:
            rows = self.db.execute(
                "SELECT origin, seq, kind, channel, sender, body, clock, ts_ns FROM entries "
                "WHERE origin = ? AND seq > ? ORDER BY seq",
                (origin, vector.get(origin, 0)),
            )
            entries.extend(self._entry(row) for row in rows)
        return entries

    def _insert(self, entry: pb.ReplicaEntry) -> None:
        kind = entry.WhichOneof("payload")
        if kind == "channel":
            channel, sender, body = entry.channel.name, None, None
        else:
            p = entry.publication
            channel, sender, body = p.channel, p.sender, p.message

        self.db.execute(
            "INSERT INTO entries (origin, seq, kind, channel, sender, body, clock, ts_ns) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (entry.origin, entry.seq, kind, channel, sender, body, entry.clock,
             to_ns(entry.timestamp)),
        )
        self.vector[entry.origin] = entry.seq

    @staticmethod
    def _entry(row) -> pb.ReplicaEntry:
        origin, seq, kind, channel, sender, body, clock, ts_ns = row
        entry = pb.ReplicaEntry(origin=origin, seq=seq, clock=clock, timestamp=to_ts(ts_ns))
        if kind == "channel":
            entry.channel.name = channel
        else:
            entry.publication.channel = channel
            entry.publication.sender = sender
            entry.publication.message = body
        return entry

    # ---------- Registro de mensagens ----------

    def append_log(
        self, direction: str, kind: str, payload: bytes, message_ts: Timestamp | None, clock: int
    ) -> pb.LogEntry:
        """Registra uma mensagem trocada com um cliente através deste servidor."""
        entry = pb.LogEntry(
            origin=self.origin, seq=self.log_vector.get(self.origin, 0) + 1,
            direction=direction, kind=kind, payload=payload, clock=clock,
        )
        if message_ts is not None:
            entry.message_ts.CopyFrom(message_ts)
        self._insert_log(entry)
        return entry

    def apply_log(self, entry: pb.LogEntry) -> str:
        """Aplica um registro de outro servidor. Só aceita o próximo da sua origem."""
        last = self.log_vector.get(entry.origin, 0)
        if entry.seq <= last:
            return DUPLICATE
        if entry.seq > last + 1:
            return GAP
        self._insert_log(entry)
        return APPLIED

    def log_entries_since(self, log_vector) -> list[pb.LogEntry]:
        entries = []
        for origin in self.log_vector:
            rows = self.db.execute(
                "SELECT origin, seq, direction, kind, payload, message_ts_ns, clock "
                "FROM message_log WHERE origin = ? AND seq > ? ORDER BY seq",
                (origin, log_vector.get(origin, 0)),
            )
            for origin, seq, direction, kind, payload, ts_ns, clock in rows:
                entry = pb.LogEntry(
                    origin=origin, seq=seq, direction=direction, kind=kind,
                    payload=payload, clock=clock,
                )
                if ts_ns is not None:
                    entry.message_ts.CopyFrom(to_ts(ts_ns))
                entries.append(entry)
        return entries

    def _insert_log(self, entry: pb.LogEntry) -> None:
        ts_ns = to_ns(entry.message_ts) if entry.HasField("message_ts") else None
        self.db.execute(
            "INSERT INTO message_log "
            "(origin, seq, direction, kind, payload, message_ts_ns, clock) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (entry.origin, entry.seq, entry.direction, entry.kind, entry.payload,
             ts_ns, entry.clock),
        )
        self.log_vector[entry.origin] = entry.seq
