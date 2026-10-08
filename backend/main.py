"""Home & Hustle Planner backend: data sync + Telegram reminders."""
import asyncio
import json
import os
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
from fastapi import Body, Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
SECRET = os.getenv("APP_SECRET", "")
TZ = ZoneInfo(os.getenv("TZ_NAME", "Africa/Blantyre"))
DB = os.getenv("DB_PATH", "planner.db")
DATABASE_URL = os.getenv("DATABASE_URL", "")  # Postgres (e.g. Neon) when set, else local SQLite file
PG = DATABASE_URL.startswith("postgres")
if PG:
    import psycopg
EVENING = os.getenv("EVENING_TIME", "19:00")
ORIGINS = [o.strip() for o in os.getenv("ALLOW_ORIGINS", "*").split(",") if o.strip()]
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


# ---------- database ----------
# Everything the scheduler needs is cached in memory, so the database is only
# touched on startup and when something changes (lets a free Postgres sleep).
CACHE = {"state": None, "sent": None, "chat": None}


def q(sql, args=(), one=False):
    if PG:
        with psycopg.connect(DATABASE_URL, connect_timeout=15) as c:
            cur = c.execute(sql.replace("?", "%s"), args)
            rows = cur.fetchall() if cur.description else []
    else:
        c = sqlite3.connect(DB)
        try:
            cur = c.execute(sql, args)
            c.commit()
            rows = cur.fetchall()
        finally:
            c.close()
    return (rows[0] if rows else None) if one else rows


def init():
    q("create table if not exists kv(k text primary key, v text)")


def kv_get(k, default=None):
    r = q("select v from kv where k=?", (k,), one=True)
    return json.loads(r[0]) if r else default


def kv_set(k, v):
    q("insert into kv(k,v) values(?,?) on conflict(k) do update set v=excluded.v", (k, json.dumps(v)))


def state():
    if CACHE["state"] is None:
        CACHE["state"] = kv_get("state", {}) or {}
    return CACHE["state"]


def seen(k):
    if CACHE["sent"] is None:
        CACHE["sent"] = set(kv_get("sent", []))
    return k in CACHE["sent"]


def mark(k):
    seen(k)
    cutoff = (datetime.now(TZ) - timedelta(days=7)).date().isoformat()
    CACHE["sent"] = {x for x in CACHE["sent"] if x[:10] >= cutoff} | {k}
    kv_set("sent", sorted(CACHE["sent"]))


# ---------- telegram ----------
def chat_id():
    env = os.getenv("TELEGRAM_CHAT_ID")
    if env:
        return env
    if CACHE["chat"] is None:
        CACHE["chat"] = str(kv_get("chat_id") or "")
    return CACHE["chat"] or None


async def tg(text):
    cid = chat_id()
    if not TOKEN or not cid:
        print("Telegram not ready (missing token or chat id)")
        return False
    try:
        async with httpx.AsyncClient(timeout=15) as h:
            r = await h.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                             json={"chat_id": cid, "text": text})
        if r.status_code != 200:
            print("Telegram error:", r.text)
        return r.status_code == 200
    except Exception as e:
        print("Telegram send failed:", e)
        return False


async def find_chat_id():
    if chat_id():
        return
    async with httpx.AsyncClient(timeout=15) as h:
        r = await h.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates")
    for u in reversed(r.json().get("result", [])):
        m = u.get("message")
        if m:
            kv_set("chat_id", m["chat"]["id"])
            CACHE["chat"] = str(m["chat"]["id"])
            print(f"Found your chat id: {m['chat']['id']}  (add TELEGRAM_CHAT_ID={m['chat']['id']} to .env to lock it)")
            return
    print("No chat id yet: open your bot in Telegram and send it a message, then restart.")


# ---------- message builders ----------
def mins(t):
    try:
        h, m = str(t).split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return None


def today_tasks(s, n):
    return sorted([t for t in s.get("tasks", []) if t.get("day") == n.weekday()],
                  key=lambda t: t.get("time", ""))


def task_status(s, t, n):
    return ((s.get("log") or {}).get(n.date().isoformat()) or {}).get(t.get("id"), "")


def stock_state(s, name):
    n = str(name).strip().lower()
    for i in s.get("stock", []):
        if str(i.get("name", "")).strip().lower() == n:
            qty = i.get("qty", 0)
            return "out" if qty <= 0 else "low" if qty <= i.get("min", 0) else "ok"
    return "none"


def menu_needs(s, weekday):
    m = (s.get("menu") or {}).get(str(weekday)) or {}
    out = []
    for k, label in (("b", "breakfast"), ("l", "lunch"), ("d", "dinner")):
        for ing in m.get(k + "n") or []:
            st = stock_state(s, ing)
            if st in ("out", "low"):
                out.append(f"{ing} ({st}) for {label}")
    return out


def low_items(s):
    return [i for i in s.get("stock", []) if i.get("qty", 0) <= i.get("min", 0)]


def unpaid(s):
    return [x for x in s.get("biz", []) if not x.get("paid")]


def money(s, n):
    return f"{(s.get('set') or {}).get('cur', 'MK')} {int(n):,}"


def cups(x):
    return sum((x.get("cups") or {}).values())


def morning_text(s, n):
    tt = [t for t in today_tasks(s, n) if task_status(s, t, n) != "done"]
    m = (s.get("menu") or {}).get(str(n.weekday())) or {}
    lo = low_items(s)
    L = [f"☀️ Good morning! It's {DAYS[n.weekday()]}.", "", "📅 Plans today:"]
    L += [f"  • {t.get('time')} {t.get('title')}" for t in tt] or ["  • Nothing planned"]
    L += ["", "🍲 Menu:", f"  Breakfast: {m.get('b') or '-'}", f"  Lunch: {m.get('l') or '-'}",
          f"  Dinner: {m.get('d') or '-'}"]
    if lo:
        L += ["", "🛒 Low or out at home:"] + [f"  • {i.get('name')} ({i.get('qty')} {i.get('unit', '')})" for i in lo]
    need = menu_needs(s, n.weekday())
    if need:
        L += ["", "🛒 Today's menu needs:"] + [f"  • {x}" for x in need]
    owed = unpaid(s)
    if owed:
        L += ["", f"💰 Customers owe you {money(s, sum(x.get('amt', 0) for x in owed))}"]
    return "\n".join(L)


def evening_text(s, n):
    today = n.date().isoformat()
    rows = [x for x in s.get("biz", []) if x.get("date") == today]
    inc = sum(x.get("amt", 0) for x in rows)
    exp = sum(x.get("exp", 0) for x in rows)
    tasks = today_tasks(s, n)
    left = [t for t in tasks if task_status(s, t, n) != "done"]
    L = ["🌙 Evening summary", "", f"🥜 Jobs: {len([x for x in rows if x.get('kind') != 'exp'])} | Cups: {sum(cups(x) for x in rows)}",
         f"💵 Income: {money(s, inc)} | Profit: {money(s, inc - exp)}"]
    L += [f"✅ Plans done: {len(tasks) - len(left)}/{len(tasks)}"]
    if left:
        L += ["", "📋 Not done:"] + [f"  • {t.get('time')} {t.get('title')}" for t in left]
    tmr = menu_needs(s, (n + timedelta(days=1)).weekday())
    if tmr:
        L += ["", "🛒 Tomorrow's menu needs (buy today):"] + [f"  • {x}" for x in tmr]
    owed = unpaid(s)
    if owed:
        L += ["", f"💰 Unpaid: {money(s, sum(x.get('amt', 0) for x in owed))} from {len(owed)} job(s)"]
    return "\n".join(L)


# ---------- scheduler ----------
async def check():
    s = state()
    if not s or not TOKEN or not chat_id():
        return
    n = datetime.now(TZ)
    day = n.date().isoformat()
    now_m = n.hour * 60 + n.minute

    for t in today_tasks(s, n):
        tm, key = mins(t.get("time")), f"{day}:t:{t.get('id')}"
        if task_status(s, t, n) == "done" or tm is None or seen(key):
            continue
        if 0 <= now_m - tm <= 10 and await tg(f"⏰ {t.get('title')}\nScheduled for {t.get('time')}"):
            mark(key)

    morning = mins((s.get("set") or {}).get("morning", "07:00"))
    if morning is not None and 0 <= now_m - morning <= 60 and not seen(f"{day}:morning"):
        if await tg(morning_text(s, n)):
            mark(f"{day}:morning")

    evening = mins(EVENING)
    if evening is not None and 0 <= now_m - evening <= 60 and not seen(f"{day}:evening"):
        if await tg(evening_text(s, n)):
            mark(f"{day}:evening")


async def loop():
    while True:
        try:
            await check()
        except Exception as e:
            print("check error:", e)
        await asyncio.sleep(30)


# ---------- api ----------
@asynccontextmanager
async def lifespan(app):
    init()
    if not SECRET:
        print("WARNING: APP_SECRET is not set, all API calls will be refused.")
    if TOKEN:
        try:
            await find_chat_id()
        except Exception as e:
            print("Could not look up chat id:", e)
    task = asyncio.create_task(loop())
    yield
    task.cancel()


app = FastAPI(title="Home & Hustle Planner", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=ORIGINS, allow_methods=["*"], allow_headers=["*"])


def auth(x_api_key: str = Header(default="")):
    if not SECRET or not secrets.compare_digest(x_api_key, SECRET):
        raise HTTPException(401, "Bad or missing API key")


@app.get("/health")
def health():
    return {"ok": True, "time": datetime.now(TZ).isoformat(timespec="seconds"),
            "telegram_ready": bool(TOKEN and chat_id())}


@app.get("/api/state", dependencies=[Depends(auth)])
def get_state():
    return {"state": state(), "updated": kv_get("updated", 0)}


@app.put("/api/state", dependencies=[Depends(auth)])
async def put_state(req: Request, payload: dict = Body(...)):
    if int(req.headers.get("content-length", "0") or 0) > 2_000_000:
        raise HTTPException(413, "Too large")
    s = payload.get("state")
    if not isinstance(s, dict):
        raise HTTPException(400, "state must be an object")
    (s.get("set") or {}).pop("key", None)  # never store the Groq key on the server
    kv_set("state", s)
    CACHE["state"] = s
    kv_set("updated", time.time())
    return {"ok": True, "updated": kv_get("updated")}


@app.post("/api/test", dependencies=[Depends(auth)])
async def test():
    return {"sent": await tg("✅ Planner server is connected. Reminders will arrive here.")}