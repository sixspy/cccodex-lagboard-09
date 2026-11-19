
import hashlib, secrets, sqlite3, threading
from contextlib import contextmanager
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
# 库文件落在禁写仓区：冻结台账、水位与到达流水同一库，禁写门闩才能同事务。
VAULT = DATA / "freeze"
DB_PATH = VAULT / "app.db"
LEGACY_DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}
# 同源在途写事务计数：只有同源两路重叠且后交抢锁失败，才算冻结禁写争用。
_INFLIGHT: dict[str, int] = {}
_INFLIGHT_LOCK = threading.Lock()

def _inflight_enter(source: str) -> int:
    with _INFLIGHT_LOCK:
        _INFLIGHT[source] = _INFLIGHT.get(source, 0) + 1
        return _INFLIGHT[source]

def _inflight_exit(source: str) -> None:
    with _INFLIGHT_LOCK:
        n = _INFLIGHT.get(source, 1) - 1
        if n <= 0:
            _INFLIGHT.pop(source, None)
        else:
            _INFLIGHT[source] = n

def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

@contextmanager
def write_tx():
    """入库唯一写口：BEGIN IMMEDIATE 即冻结禁写锁。

    返回 (conn, contended)。contended 取自真实锁获取：零等待抢锁失败即说明
    另一路已占住禁写锁，本次为后交。
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=0")
    try:
        conn.execute("BEGIN IMMEDIATE")
        contended = False
    except sqlite3.OperationalError:
        contended = True
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn, contended
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

def _migrate_legacy_db():
    """旧库仅在禁写仓区缺库时一次性迁入，之后仓区库为准。"""
    if DB_PATH.exists() or not LEGACY_DB_PATH.exists():
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    import shutil
    shutil.copy2(LEGACY_DB_PATH, DB_PATH)

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "源到达SLA"
SKELETON = "多源到达时钟对照 SLA 分级，超限则冻结或降级输出，补齐后恢复并对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    _migrate_legacy_db()
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS sources(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE NOT NULL,
          sla_minutes INTEGER NOT NULL,
          output_mode TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS arrivals(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          source_clock TEXT NOT NULL,
          ingest_clock TEXT NOT NULL,
          lag_minutes INTEGER NOT NULL,
          level TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS catchups(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          status TEXT NOT NULL,
          restored_mode TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS freeze_audit(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          kind TEXT NOT NULL,
          mode TEXT NOT NULL,
          source_clock TEXT,
          ingest_clock TEXT,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingest_tasks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          status TEXT NOT NULL,
          kind TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermarks(
          source TEXT PRIMARY KEY,
          state TEXT NOT NULL,
          last_source_clock TEXT,
          frozen_at TEXT,
          last_ingest_clock TEXT,
          updated_at TEXT NOT NULL
        );
        """)
        if conn.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"] == 0:
            conn.executemany("INSERT INTO sources(name,sla_minutes,output_mode) VALUES (?,?,?)", [
                ("订单源", 15, "可写"), ("支付源", 20, "降级只读"), ("物流源", 30, "冻结"),
            ])
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("支付源","2026-11-16T08:00:00","2026-11-16T08:35:00",35,"告警"))
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("物流源","2026-11-16T07:00:00","2026-11-16T09:10:00",130,"违约"))
        _sync_watermarks(conn)


def _sync_watermarks(conn):
    """水位页与源目录同态：源目录每变的态都映到水位表。"""
    now = datetime.now().isoformat(timespec="seconds")
    for src in conn.execute("SELECT name, output_mode FROM sources").fetchall():
        wm = conn.execute("SELECT * FROM watermarks WHERE source=?", (src["name"],)).fetchone()
        state = src["output_mode"]
        if wm is None:
            conn.execute(
                "INSERT INTO watermarks(source,state,last_source_clock,frozen_at,last_ingest_clock,updated_at)"
                " VALUES (?,?,NULL,?,NULL,?)",
                (src["name"], state, now if state == "冻结" else None, now),
            )
        elif wm["state"] != state:
            conn.execute(
                "UPDATE watermarks SET state=?, frozen_at=?, updated_at=? WHERE source=?",
                (state, now if state == "冻结" else None, now, src["name"]),
            )


def _freeze_state(conn, source):
    row = conn.execute(
        "SELECT s.output_mode, w.state, w.last_source_clock FROM sources s"
        " LEFT JOIN watermarks w ON w.source=s.name WHERE s.name=?",
        (source,),
    ).fetchone()
    return row


def _audit(conn, source, kind, mode, detail, source_clock=None, ingest_clock=None):
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO freeze_audit(source,kind,mode,source_clock,ingest_clock,detail,created_at)"
        " VALUES (?,?,?,?,?,?,?)",
        (source, kind, mode, source_clock, ingest_clock, detail, now),
    )


def _task(conn, source, status, kind, detail):
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO ingest_tasks(source,status,kind,detail,created_at) VALUES (?,?,?,?,?)",
        (source, status, kind, detail, now),
    )


def _watermark_touch(conn, source, state, source_clock=None, ingest_clock=None):
    """水位只前进不回退；冻结期禁写，解冻后才可越过。"""
    now = datetime.now().isoformat(timespec="seconds")
    wm = conn.execute("SELECT * FROM watermarks WHERE source=?", (source,)).fetchone()
    prev = wm["last_source_clock"] if wm else None
    nxt = source_clock if (prev is None or (source_clock and source_clock > prev)) else prev
    conn.execute(
        "INSERT INTO watermarks(source,state,last_source_clock,frozen_at,last_ingest_clock,updated_at)"
        " VALUES (?,?,?,?,?,?)"
        " ON CONFLICT(source) DO UPDATE SET state=excluded.state,"
        " last_source_clock=excluded.last_source_clock, frozen_at=excluded.frozen_at,"
        " last_ingest_clock=excluded.last_ingest_clock, updated_at=excluded.updated_at",
        (source, state, nxt, now if state == "冻结" else None,
         ingest_clock or (wm["last_ingest_clock"] if wm else None), now),
    )


def level_of(lag, sla):
    if lag <= sla: return "正常"
    if lag <= sla * 2: return "告警"
    return "违约"

def mode_of(level):
    return {"正常":"可写", "告警":"降级只读", "违约":"冻结"}[level]

@app.on_event("startup")
def _s(): init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "sources": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/sources")
def sources(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM sources ORDER BY id").fetchall()]}

class ArrivalIn(BaseModel):
    source: str
    source_clock: str
    ingest_clock: str
    force: bool = False


@app.post("/api/arrivals")
def add_arrival(body: ArrivalIn, authorization: str | None = Header(default=None)):
    """冻结源禁写入库门闩（甲路径）：同事务锁行、冻结即拒插并记审计。

    冻结禁写甲/乙两径不混用：本仓只走甲径，不做带版本戳的条件更新。
    """
    require_user(authorization)
    def parse(t):
        return datetime.fromisoformat(t)
    lag = int((parse(body.ingest_clock) - parse(body.source_clock)).total_seconds() // 60)
    if lag < 0: lag = 0

    rejection = None
    overlap = _inflight_enter(body.source)
    try:
        with write_tx() as (c, contended):
            src = c.execute("SELECT * FROM sources WHERE name=?", (body.source,)).fetchone()
            if not src: raise HTTPException(404, "未知源")
            lv = level_of(lag, src["sla_minutes"])
            mode = mode_of(lv)
            state = _freeze_state(c, body.source)
            frozen = "冻结" in (src["output_mode"], state["state"] if state else None)
            # 争用 = 同源已有在途写事务 且 本次零等待抢锁失败（后交）。
            contention = contended and overlap > 1

            if body.force:
                # 偏离冻结禁写说明的绕写请求：一律驳回并留“禁写偏了”。
                _audit(c, body.source, "禁写偏了", src["output_mode"],
                       "绕写请求被驳回，零新增", body.source_clock, body.ingest_clock)
                _task(c, body.source, "偏了", "冻结禁写", "force 绕写被驳回，未插入")
                rejection = "冻结禁写：偏离说明的绕写已驳回，零新增"
            elif frozen:
                # 先交占住禁写锁即留“冻结禁写”；同源后交抢锁失败只记“冻结禁写争用”。
                kind = "冻结禁写争用" if contention else "冻结禁写"
                _audit(c, body.source, kind, src["output_mode"],
                       "冻结态拒插，到达未入库" + ("（双路争用后交）" if contention else ""),
                       body.source_clock, body.ingest_clock)
                _task(c, body.source, "争用" if contention else "拒写", "冻结禁写",
                      "后交争用已留痕，零新增" if contention else "冻结态拒插，零新增")
                rejection = ("冻结禁写：源已冻结，后交争用，新到达拒绝入库" if contention
                             else "冻结禁写：源已冻结，新到达拒绝入库")
            else:
                c.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                          (body.source, body.source_clock, body.ingest_clock, lag, lv))
                c.execute("UPDATE sources SET output_mode=? WHERE name=?", (mode, body.source))
                _watermark_touch(c, body.source, mode, body.source_clock, body.ingest_clock)
                _task(c, body.source, "入库", "到达登记", f"滞后 {lag} 分 · {lv}")
    finally:
        _inflight_exit(body.source)

    if rejection:
        # 事务已提交（审计与任务留痕在案），此处才拒；冻结源零新增。
        raise HTTPException(409, rejection)
    return {"lag_minutes": lag, "level": lv, "output_mode": mode}


@app.get("/api/arrivals")
def arrivals(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM arrivals ORDER BY id DESC").fetchall()]}


@app.get("/api/freeze/audit")
def freeze_audit_page(authorization: str | None = Header(default=None)):
    """审计附页：冻结禁写、争用、偏了、解冻全在此。"""
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM freeze_audit ORDER BY id DESC").fetchall()]}


@app.get("/api/freeze/tasks")
def freeze_tasks_page(authorization: str | None = Header(default=None)):
    """任务列表：拒写/争用/偏了/入库同态可见。"""
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM ingest_tasks ORDER BY id DESC").fetchall()]}


@app.get("/api/freeze/watermarks")
def freeze_watermarks_page(authorization: str | None = Header(default=None)):
    """水位页：与源目录同态，冻结期水位不越过。"""
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM watermarks ORDER BY source").fetchall()]}


@app.post("/api/catchup/{source}")
def catchup(source: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    now = datetime.now().isoformat(timespec="seconds")
    with write_tx() as (c, _contended):
        src = c.execute("SELECT * FROM sources WHERE name=?", (source,)).fetchone()
        if not src: raise HTTPException(404, "未知源")
        c.execute("UPDATE sources SET output_mode='可写' WHERE name=?", (source,))
        c.execute("INSERT INTO catchups(source,status,restored_mode,note,created_at) VALUES (?,?,?,?,?)",
                  (source, "成功", "可写", "补齐后恢复可写", now))
        _watermark_touch(c, source, "可写")
        _audit(c, source, "解冻", "可写", "补齐后整源解冻，恢复入库", None, None)
        _task(c, source, "解冻", "补齐恢复", "整源解冻为可写")
        return {"ok": True, "output_mode": "可写"}

@app.get("/api/catchups")
def catchups(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM catchups ORDER BY id DESC").fetchall()]}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
