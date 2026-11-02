#!/usr/bin/env python3
"""Polymarket BTTS-No bot (Polymarket-only: sports stream + CLOB prices).

Rule: team leads by >= MIN_LEAD goals, trailing team has 0 goals,
leader's win price >= LEADER_MIN_PRICE (odds ~1.10 or lower),
trailing team's win price <= TRAILER_MAX_PRICE (odds ~50+)
-> buy "No" on the full-match Both Teams To Score market.

Score and minute come from Polymarket's sports WebSocket (no API-Football).

Run:  python bot.py --discover   (see which BTTS markets Polymarket has)
      python bot.py              (run the bot; DRY_RUN=true by default)
Stop: Ctrl+C  (not Ctrl+Z)
"""
import os, re, json, time, sqlite3, difflib, argparse, logging, threading, asyncio
import requests
import websockets
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("bot")

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
SPORTS_WS = "wss://sports-api.polymarket.com/ws"


def envf(k, d):
    return float(os.getenv(k, d))


DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
STAKE = envf("STAKE_USD", 2)
DAILY_CAP = envf("DAILY_CAP_USD", 10)
MIN_LEAD = int(envf("MIN_LEAD", 2))
MIN_MINUTE = int(envf("MIN_MINUTE", 45))
LEADER_MIN_PRICE = envf("LEADER_MIN_PRICE", 0.91)    # odds 1.10
TRAILER_MAX_PRICE = envf("TRAILER_MAX_PRICE", 0.02)  # odds 50
MAX_NO_PRICE = envf("MAX_NO_PRICE", 0.97)            # skip if no payout left
MIN_NO_PRICE = envf("MIN_NO_PRICE", 0.80)            # skip if No is already cheap (score may be stale)
POLL = int(envf("POLL_SECONDS", 15))

db = sqlite3.connect("bot.db", check_same_thread=False)
db.execute("""CREATE TABLE IF NOT EXISTS trades (
    fixture_id TEXT PRIMARY KEY, match TEXT, score TEXT, minute INT,
    leader_price REAL, trailer_price REAL, no_price REAL, usd REAL,
    dry_run INT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)""")
db.commit()


# ---------- helpers ----------
def tg(msg):
    t, c = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if t and c:
        try:
            requests.post(f"https://api.telegram.org/bot{t}/sendMessage",
                          data={"chat_id": c, "text": msg}, timeout=10)
        except Exception:
            pass


def norm(s):
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    s = re.sub(r"\b(fc|cf|afc|sc|ac|club|de|the)\b", " ", s)
    return " ".join(s.split())


def same(a, b):
    a, b = norm(a), norm(b)
    if not a or not b:
        return False
    return a == b or a in b or b in a or difflib.SequenceMatcher(None, a, b).ratio() > 0.8


def j(v):
    return json.loads(v) if isinstance(v, str) else v


def geoblocked():
    try:
        return requests.get("https://polymarket.com/api/geoblock", timeout=10).json().get("blocked", False)
    except Exception:
        return False


# ---------- Polymarket markets ----------
def is_btts(m):
    """Full-match BTTS only. Excludes 'Both Teams to Score in First/Second Half'."""
    q = m.get("question", "").lower().strip()
    return q.rsplit(":", 1)[-1].strip() == "both teams to score"


def load_btts_events():
    found = {}
    for off in range(0, 600, 100):
        r = requests.get(f"{GAMMA}/events", params={
            "active": "true", "closed": "false", "tag_slug": "soccer",
            "limit": 100, "offset": off}, timeout=20).json()
        if not r:
            break
        for e in r:
            if any(is_btts(m) for m in e.get("markets", [])):
                found[e["id"]] = e
    return list(found.values())


def teams_of(e):
    parts = re.split(r"\s+vs\.?\s+", e.get("title", ""), flags=re.I)
    if len(parts) != 2:
        return None
    clean = lambda s: re.sub(r"\s+-\s+.*$", "", s).strip()
    return clean(parts[0]), clean(parts[1])


def match_event(events, home, away):
    """Returns (event, polymarket_name_of_home, polymarket_name_of_away)."""
    for e in events:
        t = teams_of(e)
        if not t:
            continue
        if same(home, t[0]) and same(away, t[1]):
            return e, t[0], t[1]
        if same(home, t[1]) and same(away, t[0]):
            return e, t[1], t[0]
    return None


def btts_market(e):
    return next((m for m in e["markets"] if is_btts(m)), None)


def win_market(e, team):
    for m in e["markets"]:
        q = m.get("question", "").lower()
        if "win" in q and "draw" not in q and "both teams" not in q and norm(team) in norm(q):
            return m
    return None


_ev_cache = {}  # game id -> (timestamp, [events])


def search_events(q):
    r = requests.get(f"{GAMMA}/public-search", params={"q": q}, timeout=15).json()
    return [e for e in r.get("events", []) if not e.get("closed")]


def find_events(gid, home, away):
    """Find every Polymarket event for this match (main + 'More Markets')."""
    hit = _ev_cache.get(gid)
    if hit and time.time() - hit[0] < (600 if hit[1] else 120):
        return hit[1]
    found = {}
    for q in dict.fromkeys([home, norm(home), away, norm(away)]):
        if not q:
            continue
        try:
            for e in search_events(q):
                if match_event([e], home, away):
                    found[e["id"]] = e
        except Exception as ex:
            log.info(f"[search error] {q}: {ex}")
        if any(btts_market(e) for e in found.values()):
            break
    # BTTS usually sits in a sibling "<slug>-more-markets" event; fetch it by slug
    for e in list(found.values()):
        slug = e.get("slug") or ""
        if slug and not slug.endswith("-more-markets") and not any(btts_market(x) for x in found.values()):
            try:
                r = requests.get(f"{GAMMA}/events/slug/{slug}-more-markets", timeout=15)
                if r.status_code == 200:
                    mm = r.json()
                    if isinstance(mm, dict) and mm.get("id") and not mm.get("closed"):
                        found[mm["id"]] = mm
            except Exception as ex:
                log.info(f"[slug error] {slug}: {ex}")
    evs = list(found.values())
    _ev_cache[gid] = (time.time(), evs)
    return evs


def token(m, side):
    outs = [o.lower() for o in j(m["outcomes"])]
    return j(m["clobTokenIds"])[outs.index(side)]


def mid(pub, tok):
    return float(pub.get_midpoint(tok)["mid"])


def best_ask(pub, tok):
    asks = pub.get_order_book(tok).asks
    return min((float(a.price) for a in asks), default=None)


# ---------- live scores: Polymarket sports stream ----------
games = {}  # gameId -> latest update dict


async def _sports_loop():
    backoff = 1
    while True:
        try:
            async with websockets.connect(SPORTS_WS, ping_interval=None) as ws:
                log.info("Sports stream connected")
                backoff = 1
                async for raw in ws:
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    if raw.strip().lower() == "ping":   # app-level heartbeat
                        await ws.send("pong")
                        continue
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    for g in (msg if isinstance(msg, list) else [msg]):
                        if isinstance(g, dict) and "gameId" in g:
                            games[g["gameId"]] = g
        except Exception as ex:
            log.warning(f"Sports stream dropped: {ex} | retry in {backoff}s")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


def start_sports_stream():
    threading.Thread(target=lambda: asyncio.run(_sports_loop()), daemon=True).start()


def parse_score(s):
    m = re.match(r"\s*(\d+)\s*-\s*(\d+)", s or "")
    return (int(m[1]), int(m[2])) if m else None


def parse_minute(g):
    m = re.match(r"\s*(\d+)", str(g.get("elapsed") or ""))
    if not m:
        return None
    minute = int(m[1])
    if g.get("period") == "2H" and minute < 45:   # in case the clock is per-half
        minute += 45
    return minute


# ---------- trading ----------
def get_trading_client():
    c = ClobClient(CLOB, key=os.environ["PRIVATE_KEY"], chain_id=137,
                   signature_type=int(os.getenv("SIG_TYPE", "1")),
                   funder=os.getenv("FUNDER_ADDRESS"))
    c.set_api_creds(c.create_or_derive_api_creds())
    return c


def place(trader, tok, usd):
    if DRY_RUN:
        return "dry-run"
    order = trader.create_market_order(MarketOrderArgs(token_id=tok, amount=usd, side=BUY))
    return trader.post_order(order, OrderType.FOK)


def spent_today():
    return db.execute("SELECT COALESCE(SUM(usd),0) FROM trades WHERE date(ts)=date('now') AND dry_run=?",
                      (int(DRY_RUN),)).fetchone()[0]


warned, raw_logged = set(), set()


def tick(pub, trader):
    for g in list(games.values()):
        if not g.get("live") or g.get("ended") or g.get("period") not in ("1H", "2H"):
            continue
        sc = parse_score(g.get("score"))
        if not sc:
            continue
        gh, ga = sc
        lead, trail = max(gh, ga), min(gh, ga)
        if trail != 0 or lead < MIN_LEAD:
            continue

        gid = f"pm:{g['gameId']}"
        home, away = g.get("homeTeam") or "", g.get("awayTeam") or ""
        if gid not in raw_logged:   # so you can check the clock format once
            log.info(f"[lead] {home} {g.get('score')} {away} period={g.get('period')} elapsed={g.get('elapsed')!r}")
            raw_logged.add(gid)

        minute = parse_minute(g)
        if minute is None or minute < MIN_MINUTE:
            continue
        if db.execute("SELECT 1 FROM trades WHERE fixture_id=?", (gid,)).fetchone():
            continue

        evs = find_events(gid, home, away)
        bm_ev = next((e for e in evs if btts_market(e)), None)
        if not bm_ev:
            if gid not in warned:
                desc = "; ".join(f"{e.get('title')} [{e.get('slug')}] ({len(e.get('markets', []))} mkts)" for e in evs)
                log.info(f"[skip] no Polymarket BTTS market for {home} v {away} | found: {desc or 'none'}")
                warned.add(gid)
            continue
        _, pm_home, pm_away = match_event([bm_ev], home, away)
        leader, trailer = (pm_home, pm_away) if gh > ga else (pm_away, pm_home)
        bm = btts_market(bm_ev)
        lm = next((m for e in evs for m in [win_market(e, leader)] if m), None)
        tm = next((m for e in evs for m in [win_market(e, trailer)] if m), None)
        if not (bm and lm and tm):
            if gid not in warned:
                log.info(f"[skip] missing win markets for {home} v {away}: leader={bool(lm)} trailer={bool(tm)}")
                warned.add(gid)
            continue

        try:
            lp, tp = mid(pub, token(lm, "yes")), mid(pub, token(tm, "yes"))
            no_tok = token(bm, "no")
            ask = best_ask(pub, no_tok)
        except Exception as ex:
            log.info(f"[skip] price error {home} v {away}: {ex}")
            continue

        ok = (lp >= LEADER_MIN_PRICE and tp <= TRAILER_MAX_PRICE
              and ask is not None and MIN_NO_PRICE <= ask <= MAX_NO_PRICE)
        log.info(f"{home} {gh}-{ga} {away} {minute}' leader={lp:.3f} trailer={tp:.3f} no_ask={ask} -> {'TRIGGER' if ok else 'no'}")
        if not ok:
            continue
        if spent_today() + STAKE > DAILY_CAP:
            log.info("[skip] daily cap reached")
            continue

        res = place(trader, no_tok, STAKE)
        db.execute("INSERT INTO trades (fixture_id,match,score,minute,leader_price,trailer_price,no_price,usd,dry_run) "
                   "VALUES (?,?,?,?,?,?,?,?,?)",
                   (gid, f"{home} v {away}", f"{gh}-{ga}", minute, lp, tp, ask, STAKE, int(DRY_RUN)))
        db.commit()
        msg = f"{'[DRY] ' if DRY_RUN else ''}BTTS NO ${STAKE} @ {ask} | {home} {gh}-{ga} {away} {minute}' | {res}"
        log.info(msg)
        tg(msg)


def discover():
    events = load_btts_events()
    print(f"{len(events)} soccer events with a full-match BTTS market")
    for e in events[:15]:
        print(f"\n{e.get('title')}")
        for m in e["markets"]:
            print("   -", m.get("question"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--discover", action="store_true")
    args = ap.parse_args()
    if args.discover:
        return discover()

    if geoblocked():
        raise SystemExit("Polymarket reports this IP as geoblocked.")
    pub = ClobClient(CLOB)  # public reads, no key needed
    trader = None if DRY_RUN else get_trading_client()
    log.info(f"Bot started | DRY_RUN={DRY_RUN} stake=${STAKE} cap=${DAILY_CAP}")
    tg(f"Bot started | DRY_RUN={DRY_RUN}")

    start_sports_stream()
    last = 0
    while True:
        try:
            if time.time() - last > 300:
                last = time.time()
                live = sum(1 for g in games.values() if g.get("live"))
                log.info(f"Heartbeat | games tracked: {len(games)} | live: {live}")
            tick(pub, trader)
        except Exception:
            log.exception("tick failed")
        time.sleep(POLL)


if __name__ == "__main__":
    main()