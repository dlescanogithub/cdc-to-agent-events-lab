"""Idempotency store: rollback, uniqueness, and both backends."""

from __future__ import annotations

import threading

import pytest

from cdc_lab.errors import CdcError, CrashInjected
from cdc_lab.store import EffectRow, Store


def _effect(event_id: str = "evt-1", record_id: str = "t:0:0") -> EffectRow:
    return EffectRow(event_id, record_id, "shipment:s", 1, "applied", 1, "{}")


@pytest.fixture(params=["memory", "sqlite"])
def store(request: pytest.FixtureRequest, tmp_path: object) -> Store:
    if request.param == "memory":
        return Store.memory()
    from pathlib import Path

    assert isinstance(tmp_path, Path)
    opened = Store.sqlite(tmp_path / "cdc.sqlite")
    return opened


def test_rollback_drops_a_staged_dedupe_mark(store: Store) -> None:
    with pytest.raises(CrashInjected), store.transaction():
        store.insert_effect("c", _effect())
        raise CrashInjected("before_store_commit")
    assert store.get_effect("c", "evt-1") is None
    with store.transaction():
        store.insert_effect("c", _effect())
        store.upsert_state("c", "shipment:s", 1, '{"lsn": 1}')
        store.insert_account("c", "t:0:0", "applied")
    assert store.get_effect("c", "evt-1") is not None
    assert store.last_lsn("c", "shipment:s") == 1
    assert store.accounts("c") == {"t:0:0": "applied"}


def test_duplicate_effect_is_rejected_and_bump_counts(store: Store) -> None:
    with pytest.raises(CdcError, match="transaction"):
        store.insert_effect("c", _effect())
    assert store.apply_once("c", "evt-1", "t:0:0", "shipment:s", 1, "{}") is True
    assert store.apply_once("c", "evt-1", "t:0:1", "shipment:s", 1, "{}") is False
    with store.transaction():
        count = store.bump_effect("c", "evt-1", "{}", 2)
    assert count == 2
    with pytest.raises(CdcError), store.transaction():
        store.bump_effect("c", "missing", "{}", 1)
    store.close()


def test_threads_apply_one_effect(tmp_path: object) -> None:
    from pathlib import Path

    assert isinstance(tmp_path, Path)
    store = Store.sqlite(tmp_path / "threads.sqlite")
    barrier = threading.Barrier(8)
    results: list[bool] = []

    def worker() -> None:
        barrier.wait()
        results.append(store.apply_once("c", "evt-1", "t:0:0", "k", 1, "{}"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results.count(True) == 1
    effect = store.get_effect("c", "evt-1")
    assert effect is not None
    assert effect.apply_count == 1
    store.close()
