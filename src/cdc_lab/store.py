"""Idempotency store. Effect, dedupe mark, and outbox commit in one transaction.

SQLite (stdlib) and an in-memory variant share one API. The UNIQUE key is
`(consumer, event_id)`. A crash before commit rolls the whole transaction back,
so a dedupe mark is never durable without the effect.
"""

from __future__ import annotations

import copy
import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from cdc_lab.errors import CdcError


@dataclass(frozen=True)
class EffectRow:
    event_id: str
    record_id: str
    entity_key: str
    lsn: int
    outcome: str
    apply_count: int
    body_json: str


@dataclass(frozen=True)
class DlqRow:
    record_id: str
    original: bytes
    error_class: str
    error_code: str
    message: str
    attempts: int
    first_failure_ms: int
    last_failure_ms: int
    source_topic: str
    source_partition: int
    source_offset: int
    replayed: bool


@dataclass(frozen=True)
class OutboxRow:
    event_id: str
    entity_key: str
    rule: str
    ts_ms: int
    status: str
    coalesced_into: str | None
    payload_json: str


@dataclass(frozen=True)
class DraftRow:
    draft_id: str
    event_id: str
    entity_key: str
    body: str
    status: str
    approver: str | None


@dataclass(frozen=True)
class AttemptRow:
    attempts: int
    backoffs_ms: tuple[int, ...]
    first_failure_ms: int | None
    last_failure_ms: int | None


_EMPTY_ATTEMPT = AttemptRow(0, (), None, None)


class Store:
    """Process-local store. One transaction at a time, guarded by a lock."""

    def __init__(self, conn: sqlite3.Connection | None) -> None:
        self._conn = conn
        self._lock = threading.Lock()
        self._in_tx = False
        self._owner: int | None = None
        self._effects: dict[tuple[str, str], EffectRow] = {}
        self._states: dict[tuple[str, str], tuple[int, str]] = {}
        self._accounts: dict[tuple[str, str], str] = {}
        self._dlq: dict[tuple[str, str], DlqRow] = {}
        self._attempts: dict[tuple[str, str], AttemptRow] = {}
        self._outbox: dict[tuple[str, str], OutboxRow] = {}
        self._drafts: dict[str, DraftRow] = {}
        self._snap: dict[str, object] | None = None
        if conn is not None:
            self._init_sql()

    @classmethod
    def memory(cls) -> Store:
        return cls(None)

    @classmethod
    def sqlite(cls, path: str | Path) -> Store:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        return cls(conn)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def transaction(self) -> Iterator[None]:
        # Nested calls from the thread that already owns the transaction join it.
        # Another thread must wait; a shared boolean would let it slip in.
        if self._owner == threading.get_ident():
            yield
            return
        self._lock.acquire()
        self._owner = threading.get_ident()
        self._in_tx = True
        try:
            self._begin()
            try:
                yield
            except BaseException:
                self._rollback()
                raise
            else:
                try:
                    self._commit()
                except BaseException:
                    self._rollback()
                    raise
        finally:
            self._in_tx = False
            self._owner = None
            self._lock.release()

    def get_effect(self, consumer: str, event_id: str) -> EffectRow | None:
        if self._conn is None:
            return self._effects.get((consumer, event_id))
        row = self._conn.execute(
            "SELECT event_id, record_id, entity_key, lsn, outcome, apply_count, body_json "
            "FROM effects WHERE consumer = ? AND event_id = ?",
            (consumer, event_id),
        ).fetchone()
        if row is None:
            return None
        return EffectRow(
            str(row[0]),
            str(row[1]),
            str(row[2]),
            int(row[3]),
            str(row[4]),
            int(row[5]),
            str(row[6]),
        )

    def insert_effect(self, consumer: str, effect: EffectRow) -> None:
        self._require_tx()
        if self.get_effect(consumer, effect.event_id) is not None:
            raise CdcError(f"effect already exists for {effect.event_id}")
        if self._conn is None:
            self._effects[(consumer, effect.event_id)] = effect
            return
        self._conn.execute(
            "INSERT INTO effects (consumer, event_id, record_id, entity_key, lsn, outcome, "
            "apply_count, body_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                consumer,
                effect.event_id,
                effect.record_id,
                effect.entity_key,
                effect.lsn,
                effect.outcome,
                effect.apply_count,
                effect.body_json,
            ),
        )

    def bump_effect(
        self, consumer: str, event_id: str, body_json: str, lsn: int
    ) -> int:
        """Broken-consumer path: apply the same event id again and return the new count."""
        self._require_tx()
        current = self.get_effect(consumer, event_id)
        if current is None:
            raise CdcError(f"cannot bump missing effect {event_id}")
        updated = EffectRow(
            event_id=current.event_id,
            record_id=current.record_id,
            entity_key=current.entity_key,
            lsn=lsn,
            outcome="applied",
            apply_count=current.apply_count + 1,
            body_json=body_json,
        )
        if self._conn is None:
            self._effects[(consumer, event_id)] = updated
            return updated.apply_count
        self._conn.execute(
            "UPDATE effects SET lsn = ?, outcome = 'applied', apply_count = ?, body_json = ? "
            "WHERE consumer = ? AND event_id = ?",
            (lsn, updated.apply_count, body_json, consumer, event_id),
        )
        return updated.apply_count

    def effects(self, consumer: str) -> list[EffectRow]:
        if self._conn is None:
            return [
                row
                for (name, _), row in sorted(self._effects.items())
                if name == consumer
            ]
        rows = self._conn.execute(
            "SELECT event_id, record_id, entity_key, lsn, outcome, apply_count, body_json "
            "FROM effects WHERE consumer = ? ORDER BY event_id",
            (consumer,),
        ).fetchall()
        return [
            EffectRow(
                str(r[0]),
                str(r[1]),
                str(r[2]),
                int(r[3]),
                str(r[4]),
                int(r[5]),
                str(r[6]),
            )
            for r in rows
        ]

    def last_lsn(self, consumer: str, key: str) -> int | None:
        row = self._state(consumer, key)
        if row is None:
            return None
        return row[0]

    def state_json(self, consumer: str, key: str) -> dict[str, object] | None:
        row = self._state(consumer, key)
        if row is None:
            return None
        parsed = json.loads(row[1])
        if not isinstance(parsed, dict):
            raise CdcError("state body is not an object")
        return parsed

    def upsert_state(self, consumer: str, key: str, lsn: int, body_json: str) -> None:
        self._require_tx()
        if self._conn is None:
            self._states[(consumer, key)] = (lsn, body_json)
            return
        self._conn.execute(
            "INSERT INTO states (consumer, entity_key, lsn, body_json) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(consumer, entity_key) DO UPDATE SET lsn = excluded.lsn, "
            "body_json = excluded.body_json",
            (consumer, key, lsn, body_json),
        )

    def all_states(self, consumer: str) -> dict[str, dict[str, object]]:
        if self._conn is None:
            items = [
                (key, body)
                for (name, key), (_lsn, body) in self._states.items()
                if name == consumer
            ]
        else:
            rows = self._conn.execute(
                "SELECT entity_key, body_json FROM states WHERE consumer = ? ORDER BY entity_key",
                (consumer,),
            ).fetchall()
            items = [(str(key), str(body)) for key, body in rows]
        out: dict[str, dict[str, object]] = {}
        for key, body in items:
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                out[key] = parsed
        return out

    def get_account(self, consumer: str, record_id: str) -> str | None:
        if self._conn is None:
            return self._accounts.get((consumer, record_id))
        row = self._conn.execute(
            "SELECT bucket FROM accounted WHERE consumer = ? AND record_id = ?",
            (consumer, record_id),
        ).fetchone()
        if row is None:
            return None
        return str(row[0])

    def insert_account(self, consumer: str, record_id: str, bucket: str) -> None:
        self._require_tx()
        if self.get_account(consumer, record_id) is not None:
            raise CdcError(f"record {record_id} is already accounted")
        if self._conn is None:
            self._accounts[(consumer, record_id)] = bucket
            return
        self._conn.execute(
            "INSERT INTO accounted (consumer, record_id, bucket) VALUES (?, ?, ?)",
            (consumer, record_id, bucket),
        )

    def update_account(self, consumer: str, record_id: str, bucket: str) -> None:
        self._require_tx()
        if self.get_account(consumer, record_id) is None:
            raise CdcError(f"record {record_id} is not accounted")
        if self._conn is None:
            self._accounts[(consumer, record_id)] = bucket
            return
        self._conn.execute(
            "UPDATE accounted SET bucket = ? WHERE consumer = ? AND record_id = ?",
            (bucket, consumer, record_id),
        )

    def accounts(self, consumer: str) -> dict[str, str]:
        if self._conn is None:
            return {
                record_id: bucket
                for (name, record_id), bucket in self._accounts.items()
                if name == consumer
            }
        rows = self._conn.execute(
            "SELECT record_id, bucket FROM accounted WHERE consumer = ?",
            (consumer,),
        ).fetchall()
        return {str(record_id): str(bucket) for record_id, bucket in rows}

    def insert_dlq(self, consumer: str, row: DlqRow) -> None:
        self._require_tx()
        if self._conn is None:
            self._dlq[(consumer, row.record_id)] = row
            return
        self._conn.execute(
            "INSERT INTO dlq (consumer, record_id, original, error_class, error_code, message, "
            "attempts, first_failure_ms, last_failure_ms, source_topic, source_partition, "
            "source_offset, replayed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                consumer,
                row.record_id,
                row.original,
                row.error_class,
                row.error_code,
                row.message,
                row.attempts,
                row.first_failure_ms,
                row.last_failure_ms,
                row.source_topic,
                row.source_partition,
                row.source_offset,
                int(row.replayed),
            ),
        )

    def list_dlq(self, consumer: str, *, open_only: bool) -> list[DlqRow]:
        if self._conn is None:
            rows = [
                row for (name, _), row in sorted(self._dlq.items()) if name == consumer
            ]
        else:
            fetched = self._conn.execute(
                "SELECT record_id, original, error_class, error_code, message, attempts, "
                "first_failure_ms, last_failure_ms, source_topic, source_partition, "
                "source_offset, replayed FROM dlq WHERE consumer = ? ORDER BY record_id",
                (consumer,),
            ).fetchall()
            rows = [
                DlqRow(
                    str(r[0]),
                    bytes(r[1]),
                    str(r[2]),
                    str(r[3]),
                    str(r[4]),
                    int(r[5]),
                    int(r[6]),
                    int(r[7]),
                    str(r[8]),
                    int(r[9]),
                    int(r[10]),
                    bool(r[11]),
                )
                for r in fetched
            ]
        if open_only:
            return [row for row in rows if not row.replayed]
        return rows

    def mark_replayed(self, consumer: str, record_id: str) -> None:
        self._require_tx()
        if self._conn is None:
            current = self._dlq[(consumer, record_id)]
            self._dlq[(consumer, record_id)] = DlqRow(
                record_id=current.record_id,
                original=current.original,
                error_class=current.error_class,
                error_code=current.error_code,
                message=current.message,
                attempts=current.attempts,
                first_failure_ms=current.first_failure_ms,
                last_failure_ms=current.last_failure_ms,
                source_topic=current.source_topic,
                source_partition=current.source_partition,
                source_offset=current.source_offset,
                replayed=True,
            )
            return
        self._conn.execute(
            "UPDATE dlq SET replayed = 1 WHERE consumer = ? AND record_id = ?",
            (consumer, record_id),
        )

    def get_attempts(self, consumer: str, record_id: str) -> AttemptRow:
        if self._conn is None:
            return self._attempts.get((consumer, record_id), _EMPTY_ATTEMPT)
        row = self._conn.execute(
            "SELECT attempts, backoffs_json, first_failure_ms, last_failure_ms "
            "FROM attempts WHERE consumer = ? AND record_id = ?",
            (consumer, record_id),
        ).fetchone()
        if row is None:
            return _EMPTY_ATTEMPT
        return AttemptRow(
            int(row[0]),
            tuple(int(item) for item in json.loads(str(row[1]))),
            None if row[2] is None else int(row[2]),
            None if row[3] is None else int(row[3]),
        )

    def save_attempts(self, consumer: str, record_id: str, row: AttemptRow) -> None:
        with self.transaction():
            payload = json.dumps(list(row.backoffs_ms))
            if self._conn is None:
                self._attempts[(consumer, record_id)] = row
                return
            self._conn.execute(
                "INSERT INTO attempts (consumer, record_id, attempts, backoffs_json, "
                "first_failure_ms, last_failure_ms) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(consumer, record_id) DO UPDATE SET attempts = excluded.attempts, "
                "backoffs_json = excluded.backoffs_json, "
                "first_failure_ms = excluded.first_failure_ms, "
                "last_failure_ms = excluded.last_failure_ms",
                (
                    consumer,
                    record_id,
                    row.attempts,
                    payload,
                    row.first_failure_ms,
                    row.last_failure_ms,
                ),
            )

    def all_attempts(self, consumer: str) -> dict[str, AttemptRow]:
        if self._conn is None:
            return {
                record_id: row
                for (name, record_id), row in self._attempts.items()
                if name == consumer
            }
        rows = self._conn.execute(
            "SELECT record_id, attempts, backoffs_json, first_failure_ms, last_failure_ms "
            "FROM attempts WHERE consumer = ?",
            (consumer,),
        ).fetchall()
        return {
            str(r[0]): AttemptRow(
                int(r[1]),
                tuple(int(item) for item in json.loads(str(r[2]))),
                None if r[3] is None else int(r[3]),
                None if r[4] is None else int(r[4]),
            )
            for r in rows
        }

    def insert_outbox(self, consumer: str, row: OutboxRow) -> bool:
        self._require_tx()
        if self._conn is None:
            key = (consumer, row.event_id)
            if key in self._outbox:
                return False
            self._outbox[key] = row
            return True
        existing = self._conn.execute(
            "SELECT 1 FROM outbox WHERE consumer = ? AND event_id = ?",
            (consumer, row.event_id),
        ).fetchone()
        if existing is not None:
            return False
        self._conn.execute(
            "INSERT INTO outbox (consumer, event_id, entity_key, rule, ts_ms, status, "
            "coalesced_into, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                consumer,
                row.event_id,
                row.entity_key,
                row.rule,
                row.ts_ms,
                row.status,
                row.coalesced_into,
                row.payload_json,
            ),
        )
        return True

    def list_outbox(self, consumer: str) -> list[OutboxRow]:
        if self._conn is None:
            return [
                row
                for (name, _), row in sorted(self._outbox.items())
                if name == consumer
            ]
        rows = self._conn.execute(
            "SELECT event_id, entity_key, rule, ts_ms, status, coalesced_into, payload_json "
            "FROM outbox WHERE consumer = ? ORDER BY event_id",
            (consumer,),
        ).fetchall()
        return [
            OutboxRow(
                str(r[0]),
                str(r[1]),
                str(r[2]),
                int(r[3]),
                str(r[4]),
                None if r[5] is None else str(r[5]),
                str(r[6]),
            )
            for r in rows
        ]

    def update_outbox(
        self, consumer: str, event_id: str, status: str, coalesced_into: str | None
    ) -> None:
        self._require_tx()
        if self._conn is None:
            current = self._outbox[(consumer, event_id)]
            self._outbox[(consumer, event_id)] = OutboxRow(
                event_id=current.event_id,
                entity_key=current.entity_key,
                rule=current.rule,
                ts_ms=current.ts_ms,
                status=status,
                coalesced_into=coalesced_into,
                payload_json=current.payload_json,
            )
            return
        self._conn.execute(
            "UPDATE outbox SET status = ?, coalesced_into = ? "
            "WHERE consumer = ? AND event_id = ?",
            (status, coalesced_into, consumer, event_id),
        )

    def insert_draft(self, row: DraftRow) -> None:
        self._require_tx()
        if self._conn is None:
            self._drafts[row.draft_id] = row
            return
        self._conn.execute(
            "INSERT INTO drafts (draft_id, consumer, event_id, entity_key, body, status, approver) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                row.draft_id,
                "",
                row.event_id,
                row.entity_key,
                row.body,
                row.status,
                row.approver,
            ),
        )

    def get_draft(self, draft_id: str) -> DraftRow | None:
        if self._conn is None:
            return self._drafts.get(draft_id)
        row = self._conn.execute(
            "SELECT draft_id, event_id, entity_key, body, status, approver FROM drafts "
            "WHERE draft_id = ?",
            (draft_id,),
        ).fetchone()
        if row is None:
            return None
        return DraftRow(
            str(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
            None if row[5] is None else str(row[5]),
        )

    def set_draft_status(
        self, draft_id: str, status: str, approver: str | None
    ) -> None:
        self._require_tx()
        current = self.get_draft(draft_id)
        if current is None:
            raise CdcError(f"unknown draft {draft_id}")
        if self._conn is None:
            self._drafts[draft_id] = DraftRow(
                draft_id=current.draft_id,
                event_id=current.event_id,
                entity_key=current.entity_key,
                body=current.body,
                status=status,
                approver=approver,
            )
            return
        self._conn.execute(
            "UPDATE drafts SET status = ?, approver = ? WHERE draft_id = ?",
            (status, approver, draft_id),
        )

    def list_drafts(self) -> list[DraftRow]:
        if self._conn is None:
            return [self._drafts[key] for key in sorted(self._drafts)]
        rows = self._conn.execute(
            "SELECT draft_id, event_id, entity_key, body, status, approver FROM drafts "
            "ORDER BY draft_id"
        ).fetchall()
        return [
            DraftRow(
                str(r[0]),
                str(r[1]),
                str(r[2]),
                str(r[3]),
                str(r[4]),
                None if r[5] is None else str(r[5]),
            )
            for r in rows
        ]

    def apply_once(
        self,
        consumer: str,
        event_id: str,
        record_id: str,
        key: str,
        lsn: int,
        body_json: str,
    ) -> bool:
        """Insert the effect once. Returns False when `(consumer, event_id)` already exists."""
        with self.transaction():
            if self.get_effect(consumer, event_id) is not None:
                return False
            self.insert_effect(
                consumer,
                EffectRow(event_id, record_id, key, lsn, "applied", 1, body_json),
            )
            self.upsert_state(consumer, key, lsn, body_json)
            if self.get_account(consumer, record_id) is None:
                self.insert_account(consumer, record_id, "applied")
            return True

    def _state(self, consumer: str, key: str) -> tuple[int, str] | None:
        if self._conn is None:
            return self._states.get((consumer, key))
        row = self._conn.execute(
            "SELECT lsn, body_json FROM states WHERE consumer = ? AND entity_key = ?",
            (consumer, key),
        ).fetchone()
        if row is None:
            return None
        return int(row[0]), str(row[1])

    def _require_tx(self) -> None:
        if not self._in_tx:
            raise CdcError("store mutation requires a transaction")

    def _begin(self) -> None:
        if self._conn is None:
            self._snap = {
                "effects": copy.deepcopy(self._effects),
                "states": copy.deepcopy(self._states),
                "accounts": copy.deepcopy(self._accounts),
                "dlq": copy.deepcopy(self._dlq),
                "attempts": copy.deepcopy(self._attempts),
                "outbox": copy.deepcopy(self._outbox),
                "drafts": copy.deepcopy(self._drafts),
            }
            return
        self._conn.execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        if self._conn is None:
            self._snap = None
            return
        self._conn.execute("COMMIT")

    def _rollback(self) -> None:
        if self._conn is None:
            snap = self._snap
            if snap is None:
                return
            effects = snap["effects"]
            states = snap["states"]
            accounts = snap["accounts"]
            dlq = snap["dlq"]
            attempts = snap["attempts"]
            outbox = snap["outbox"]
            drafts = snap["drafts"]
            assert isinstance(effects, dict)
            assert isinstance(states, dict)
            assert isinstance(accounts, dict)
            assert isinstance(dlq, dict)
            assert isinstance(attempts, dict)
            assert isinstance(outbox, dict)
            assert isinstance(drafts, dict)
            self._effects = effects
            self._states = states
            self._accounts = accounts
            self._dlq = dlq
            self._attempts = attempts
            self._outbox = outbox
            self._drafts = drafts
            self._snap = None
            return
        self._conn.execute("ROLLBACK")

    def _init_sql(self) -> None:
        assert self._conn is not None
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS effects (
              consumer TEXT NOT NULL,
              event_id TEXT NOT NULL,
              record_id TEXT NOT NULL,
              entity_key TEXT NOT NULL,
              lsn INTEGER NOT NULL,
              outcome TEXT NOT NULL,
              apply_count INTEGER NOT NULL,
              body_json TEXT NOT NULL,
              PRIMARY KEY (consumer, event_id)
            );
            CREATE TABLE IF NOT EXISTS states (
              consumer TEXT NOT NULL,
              entity_key TEXT NOT NULL,
              lsn INTEGER NOT NULL,
              body_json TEXT NOT NULL,
              PRIMARY KEY (consumer, entity_key)
            );
            CREATE TABLE IF NOT EXISTS accounted (
              consumer TEXT NOT NULL,
              record_id TEXT NOT NULL,
              bucket TEXT NOT NULL,
              PRIMARY KEY (consumer, record_id)
            );
            CREATE TABLE IF NOT EXISTS dlq (
              consumer TEXT NOT NULL,
              record_id TEXT NOT NULL,
              original BLOB NOT NULL,
              error_class TEXT NOT NULL,
              error_code TEXT NOT NULL,
              message TEXT NOT NULL,
              attempts INTEGER NOT NULL,
              first_failure_ms INTEGER NOT NULL,
              last_failure_ms INTEGER NOT NULL,
              source_topic TEXT NOT NULL,
              source_partition INTEGER NOT NULL,
              source_offset INTEGER NOT NULL,
              replayed INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (consumer, record_id)
            );
            CREATE TABLE IF NOT EXISTS attempts (
              consumer TEXT NOT NULL,
              record_id TEXT NOT NULL,
              attempts INTEGER NOT NULL,
              backoffs_json TEXT NOT NULL,
              first_failure_ms INTEGER,
              last_failure_ms INTEGER,
              PRIMARY KEY (consumer, record_id)
            );
            CREATE TABLE IF NOT EXISTS outbox (
              consumer TEXT NOT NULL,
              event_id TEXT NOT NULL,
              entity_key TEXT NOT NULL,
              rule TEXT NOT NULL,
              ts_ms INTEGER NOT NULL,
              status TEXT NOT NULL,
              coalesced_into TEXT,
              payload_json TEXT NOT NULL,
              PRIMARY KEY (consumer, event_id)
            );
            CREATE TABLE IF NOT EXISTS drafts (
              draft_id TEXT PRIMARY KEY,
              consumer TEXT NOT NULL,
              event_id TEXT NOT NULL,
              entity_key TEXT NOT NULL,
              body TEXT NOT NULL,
              status TEXT NOT NULL,
              approver TEXT
            );
            """
        )
