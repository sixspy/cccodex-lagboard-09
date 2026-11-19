"""冻结禁写门闩（甲路径）验收测。

顺序要求：这两道测必须先见红，之后才允许改写入路径。
- test_frozen_source_rejects_new_arrival：冻结源写入成功即红。
- test_dual_path_frozen_source_records_contention：缺“冻结禁写争用”留痕即红。
"""
import threading
import time

import pytest
from fastapi.testclient import TestClient

import app as appmod
from app import app, init_db

FROZEN = "物流源"
OPEN = "订单源"


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _login(client: TestClient) -> dict:
    r = client.post("/api/login", json={"username": "admin", "password": "123456"})
    assert r.status_code == 200, r.text
    return {"Authorization": "Bearer " + r.json()["token"]}


def _count(source: str) -> int:
    with appmod.connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) c FROM arrivals WHERE source=?", (source,)
        ).fetchone()["c"]


def _audit() -> list[dict]:
    with appmod.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM freeze_audit ORDER BY id"
        ).fetchall()]


def _tasks() -> list[dict]:
    with appmod.connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM ingest_tasks ORDER BY id"
        ).fetchall()]


def _watermark(source: str) -> dict:
    with appmod.connect() as conn:
        row = conn.execute(
            "SELECT * FROM watermarks WHERE source=?", (source,)
        ).fetchone()
        return dict(row) if row else {}


def _mode(source: str) -> str:
    with appmod.connect() as conn:
        return conn.execute(
            "SELECT output_mode FROM sources WHERE name=?", (source,)
        ).fetchone()["output_mode"]


def _payload(source: str, ingest: str = "2026-11-19T09:01:00", **extra) -> dict:
    body = {"source": source, "source_clock": "2026-11-19T09:00:00", "ingest_clock": ingest}
    body.update(extra)
    return body


def test_frozen_source_rejects_new_arrival():
    """冻结态下入库必须同事务锁行拒插并记审计；写入成功即红。"""
    c = TestClient(app)
    h = _login(c)
    before = _count(FROZEN)
    assert _mode(FROZEN) == "冻结"

    r = c.post("/api/arrivals", headers=h, json=_payload(FROZEN))

    assert r.status_code == 409, r.text
    assert _count(FROZEN) == before, "冻结源仍插入了新到达"
    assert _mode(FROZEN) == "冻结"

    audit = _audit()
    assert any(a["source"] == FROZEN and a["kind"] == "冻结禁写" for a in audit), audit
    tasks = _tasks()
    assert tasks and tasks[-1]["source"] == FROZEN and tasks[-1]["status"] == "拒写", tasks

    wm = _watermark(FROZEN)
    assert wm and wm["state"] == "冻结", wm
    assert wm["last_source_clock"] is None, "冻结期水位被越过"


def test_dual_path_frozen_source_records_contention():
    """采集双路同时打冻结源：零新增，且后交留下“冻结禁写争用”。

    确定性两路：先交在入库写口内持锁并占住同源在途位；后交零等待抢锁失败 → 必判后交争用。
    """
    before = _count(FROZEN)
    first_holds = threading.Event()
    results: dict[str, tuple[int, dict]] = {}

    def first() -> None:
        appmod._inflight_enter(FROZEN)
        try:
            with appmod.write_tx() as (conn, _contended):
                conn.execute(
                    "INSERT INTO freeze_audit(source,kind,mode,detail,created_at)"
                    " VALUES (?,?,?,?,?)",
                    (FROZEN, "冻结禁写", "冻结", "先交占锁", "2026-11-19T09:00:00"),
                )
                first_holds.set()
                time.sleep(0.5)  # 持锁窗口，确保后交零等待抢锁必失败
        finally:
            appmod._inflight_exit(FROZEN)

    def second() -> None:
        c = TestClient(app)
        h = _login(c)
        assert first_holds.wait(timeout=10)
        r = c.post("/api/arrivals", headers=h, json=_payload(FROZEN))
        results["second"] = (r.status_code, r.json())

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start(); t2.start()
    t1.join(timeout=15); t2.join(timeout=15)
    assert not t1.is_alive() and not t2.is_alive()

    assert results["second"][0] == 409, results
    assert _count(FROZEN) == before, "抢插期间冻结源仍新增到达"

    kinds = [a["kind"] for a in _audit() if a["source"] == FROZEN]
    assert "冻结禁写争用" in kinds, f"缺禁写争用留痕: {kinds}"
    statuses = [t["status"] for t in _tasks() if t["source"] == FROZEN]
    assert "争用" in statuses, statuses


def test_other_source_write_is_not_contention():
    """异源在写不算争用：全库锁虽被占，单打冻结源仍只记“冻结禁写”。"""
    first_holds = threading.Event()

    def other() -> None:
        with appmod.write_tx() as (conn, _c):
            first_holds.set()
            time.sleep(0.5)

    t = threading.Thread(target=other)
    t.start()
    assert first_holds.wait(timeout=10)
    c = TestClient(app)
    h = _login(c)
    r = c.post("/api/arrivals", headers=h, json=_payload(FROZEN))
    t.join(timeout=15)

    assert r.status_code == 409, r.text
    kinds = [a["kind"] for a in _audit() if a["source"] == FROZEN]
    assert "冻结禁写" in kinds, kinds
    assert "冻结禁写争用" not in kinds, f"异源写被误判争用: {kinds}"


def test_forced_bypass_logs_deviation_and_zero_insert():
    """偏离冻结禁写说明的绕写请求：驳回并留“禁写偏了”。"""
    c = TestClient(app)
    h = _login(c)
    before = _count(FROZEN)

    r = c.post("/api/arrivals", headers=h, json=_payload(FROZEN, force=True))

    assert r.status_code == 409, r.text
    assert _count(FROZEN) == before
    audit = _audit()
    assert any(a["source"] == FROZEN and a["kind"] == "禁写偏了" for a in audit), audit
    assert any(t["source"] == FROZEN and t["status"] == "偏了" for t in _tasks()), _tasks()


def test_five_surfaces_same_state():
    """源目录、到达流水、审计附页、水位页、任务列表五处读面同态。"""
    c = TestClient(app)
    h = _login(c)

    frozen_before = [a["id"] for a in c.get("/api/arrivals", headers=h).json()["items"]
                     if a["source"] == FROZEN]
    assert c.post("/api/arrivals", headers=h, json=_payload(FROZEN)).status_code == 409

    sources = {s["name"]: s for s in c.get("/api/sources", headers=h).json()["items"]}
    assert sources[FROZEN]["output_mode"] == "冻结"
    arrivals = c.get("/api/arrivals", headers=h).json()["items"]
    assert [a["id"] for a in arrivals if a["source"] == FROZEN] == frozen_before
    assert c.get("/api/freeze/audit", headers=h).json()["items"]
    watermarks = c.get("/api/freeze/watermarks", headers=h).json()["items"]
    assert {w["source"]: w["state"] for w in watermarks} == {
        s["name"]: s["output_mode"] for s in sources.values()
    }
    tasks = c.get("/api/freeze/tasks", headers=h).json()["items"]
    assert tasks and tasks[-1]["source"] == FROZEN and tasks[-1]["status"] == "拒写"

    assert c.post(f"/api/catchup/{FROZEN}", headers=h).status_code == 200
    assert c.post("/api/arrivals", headers=h, json=_payload(FROZEN)).status_code == 200
    sources = {s["name"]: s for s in c.get("/api/sources", headers=h).json()["items"]}
    watermarks = {w["source"]: w for w in c.get("/api/freeze/watermarks", headers=h).json()["items"]}
    assert sources[FROZEN]["output_mode"] == watermarks[FROZEN]["state"] == "可写"
    assert watermarks[FROZEN]["last_source_clock"] == "2026-11-19T09:00:00"
    assert c.get("/api/arrivals", headers=h).json()["items"][0]["source"] == FROZEN
    assert c.get("/api/freeze/tasks", headers=h).json()["items"][0]["status"] == "入库"
    assert c.get("/api/catchups", headers=h).json()["items"][0]["source"] == FROZEN


def test_catchup_releases_freeze_then_ingest_succeeds():
    """补齐解冻后才恢复入库；五处读面同态。"""
    c = TestClient(app)
    h = _login(c)

    assert c.post(f"/api/catchup/{FROZEN}", headers=h).status_code == 200
    wm = _watermark(FROZEN)
    assert wm["state"] == "可写" and wm["frozen_at"] is None, wm
    assert any(a["kind"] == "解冻" for a in _audit() if a["source"] == FROZEN)

    before = _count(FROZEN)
    r = c.post("/api/arrivals", headers=h, json=_payload(FROZEN))
    assert r.status_code == 200, r.text
    assert _count(FROZEN) == before + 1
    status = c.get("/api/sources", headers=h).json()["items"]
    assert [s["output_mode"] for s in status if s["name"] == FROZEN] == ["可写"]
    assert _watermark(FROZEN)["last_source_clock"] == "2026-11-19T09:00:00"
    assert _tasks()[-1]["status"] == "入库"
    assert c.get("/api/arrivals", headers=h).json()["items"][0]["source"] == FROZEN


def test_sealed_note_matches_digest():
    """封档不可改口：说明正文哈希须与封档文件一致。"""
    import hashlib
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    body = (root / "ops" / "冻结禁写说明.md").read_bytes()
    marker = b"\n---\n\n" + "封档信息：".encode("utf-8")
    digest = hashlib.sha256(body[: body.index(marker) + 1]).hexdigest()
    sealed = (root / "ops" / "冻结禁写说明.sha256").read_text(encoding="utf-8").split()[0]
    assert digest == sealed


def test_lag_triggered_freeze_blocks_next_arrival():
    """违约那一刻的到达可入库并转冻结；此后的到达必须被门闩拒绝。"""
    c = TestClient(app)
    h = _login(c)
    r = c.post("/api/arrivals", headers=h, json={
        "source": OPEN, "source_clock": "2026-11-19T10:00:00", "ingest_clock": "2026-11-19T11:00:00",
    })
    assert r.status_code == 200 and r.json()["output_mode"] == "冻结", r.text
    assert _mode(OPEN) == "冻结"
    assert _watermark(OPEN)["state"] == "冻结" and _watermark(OPEN)["frozen_at"]

    before = _count(OPEN)
    r = c.post("/api/arrivals", headers=h, json=_payload(OPEN))
    assert r.status_code == 409, r.text
    assert _count(OPEN) == before, "刚冻结的源仍插入了新到达"
