from __future__ import annotations
import os, re, io, csv, json, hmac, hashlib, logging, sqlite3, threading, time, math, urllib.parse, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo
from functools import wraps
from contextlib import contextmanager

import requests
import pandas as pd
import yfinance as yf
from flask import Flask, request, jsonify, render_template_string, redirect, url_for, flash, Response
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Investment Watchdog v2 — no broker login, no order execution.
TZ = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Tokyo"))
DB_PATH = os.getenv("DATABASE_PATH", "/var/data/watchdog.db")
WATCHLIST_DEFAULT = os.getenv("WATCHLIST", "3110.T,285A.T,6994.T,9434.T,9984.T,6330.T,346A.T")
SCAN_HOUR = int(os.getenv("SCAN_HOUR_JST", "16"))
SCAN_MINUTE = int(os.getenv("SCAN_MINUTE_JST", "15"))
MAX_SYMBOLS = 80
MAX_BODY = 1_500_000
PRICE_MAX_AGE_DAYS = 5
SIGNAL_COOLDOWN_DAYS = 3
APP_VERSION = "2.0.0"

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("investment-watchdog")
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "")
db_lock = threading.RLock()
scheduler = None
scheduler_lock = threading.Lock()

def now():
    return datetime.now(TZ)

def stamp():
    return now().isoformat(timespec="seconds")

def db_connect():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=30000")
    c.execute("PRAGMA foreign_keys=ON")
    return c

@contextmanager
def db():
    with db_lock:
        c = db_connect()
        try:
            yield c
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()

def init_db():
    with db() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY,v TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS holdings(
          symbol TEXT PRIMARY KEY,name TEXT NOT NULL DEFAULT '',quantity REAL NOT NULL,
          average_cost REAL,account_type TEXT DEFAULT '',currency TEXT DEFAULT 'JPY',
          source TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sync_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,source TEXT NOT NULL,snapshot_at TEXT NOT NULL,
          digest TEXT NOT NULL UNIQUE,row_count INTEGER NOT NULL,received_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS daily_prices(
          symbol TEXT NOT NULL,market_date TEXT NOT NULL,close REAL NOT NULL,volume REAL,
          source TEXT NOT NULL,fetched_at TEXT NOT NULL,PRIMARY KEY(symbol,market_date));
        CREATE TABLE IF NOT EXISTS analyses(
          id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT NOT NULL,market_date TEXT NOT NULL,
          score INTEGER NOT NULL,label TEXT NOT NULL,price REAL NOT NULL,details TEXT NOT NULL,
          created_at TEXT NOT NULL,UNIQUE(symbol,market_date));
        CREATE TABLE IF NOT EXISTS signal_state(
          symbol TEXT PRIMARY KEY,label TEXT NOT NULL,first_seen TEXT NOT NULL,last_seen TEXT NOT NULL,
          confirmations INTEGER NOT NULL,last_alert TEXT);
        CREATE TABLE IF NOT EXISTS alerts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT NOT NULL,label TEXT NOT NULL,
          market_date TEXT NOT NULL,score INTEGER NOT NULL,message TEXT NOT NULL,
          sent INTEGER NOT NULL DEFAULT 0,send_error TEXT,created_at TEXT NOT NULL,
          UNIQUE(symbol,label,market_date));
        CREATE TABLE IF NOT EXISTS news(
          id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT NOT NULL,title TEXT NOT NULL,
          link TEXT NOT NULL,published TEXT NOT NULL,source TEXT NOT NULL,
          digest TEXT NOT NULL UNIQUE,fetched_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS jobs(
          name TEXT PRIMARY KEY,last_started TEXT,last_finished TEXT,status TEXT,detail TEXT);
        CREATE TABLE IF NOT EXISTS events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,level TEXT NOT NULL,message TEXT NOT NULL,created_at TEXT NOT NULL);
        """)
        defaults = {
          "watchlist": WATCHLIST_DEFAULT, "alert_budget_jpy":"30000",
          "max_position_pct":"15", "portfolio_value_jpy":"0",
          "max_daily_alerts":"8", "last_scan":"", "last_scan_status":"未実行",
          "last_sync":"", "last_sync_source":"", "last_news":""
        }
        for k,v in defaults.items():
            c.execute("INSERT OR IGNORE INTO settings(k,v) VALUES(?,?)",(k,v))

def get_setting(k, default=""):
    with db() as c:
        r=c.execute("SELECT v FROM settings WHERE k=?",(k,)).fetchone()
        return r["v"] if r else default

def put_setting(k,v):
    with db() as c:
        c.execute("INSERT INTO settings(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",(k,str(v)))

def log_event(level,msg):
    with db() as c:
        c.execute("INSERT INTO events(level,message,created_at) VALUES(?,?,?)",(level,str(msg)[:1500],stamp()))

def token_ok(expected, supplied):
    return bool(expected and supplied and hmac.compare_digest(str(expected),str(supplied)))

def bearer_token():
    v=request.headers.get("Authorization","")
    return v[7:].strip() if v.lower().startswith("bearer ") else ""

def require_sync(fn):
    @wraps(fn)
    def wrapped(*a,**kw):
        if not token_ok(os.getenv("SYNC_API_TOKEN",""),bearer_token()):
            return jsonify(error="Unauthorized or SYNC_API_TOKEN is not configured"),401
        return fn(*a,**kw)
    return wrapped

def require_dashboard(fn):
    @wraps(fn)
    def wrapped(*a,**kw):
        user=os.getenv("DASHBOARD_USER","")
        password=os.getenv("DASHBOARD_PASSWORD","")
        if not user or not password:
            return Response("Dashboard authentication is not configured. Set DASHBOARD_USER and DASHBOARD_PASSWORD.",503)
        auth=request.authorization
        if not auth or not hmac.compare_digest(auth.username or "",user) or not hmac.compare_digest(auth.password or "",password):
            return Response("Authentication required",401,{"WWW-Authenticate":'Basic realm="Investment Watchdog"'})
        return fn(*a,**kw)
    return wrapped

def num(v):
    if v is None or v=="":
        return None
    try:
        x=float(str(v).replace(",","").replace("円","").replace("株","").strip())
        return x if math.isfinite(x) else None
    except Exception:
        return None

def symbol_norm(v):
    s=str(v or "").strip().upper()
    if re.fullmatch(r"\d{4,5}",s):
        return s+".T"
    return s

def parse_dt(v):
    if not v:
        return None
    d=datetime.fromisoformat(str(v).replace("Z","+00:00"))
    return d.replace(tzinfo=TZ) if d.tzinfo is None else d.astimezone(TZ)

def normalize_positions(rows):
    if not isinstance(rows,list) or not rows or len(rows)>1000:
        raise ValueError("保有明細は1〜1000行で送信してください")
    result={}
    for r in rows:
        if not isinstance(r,dict):
            continue
        sym=symbol_norm(r.get("symbol") or r.get("銘柄コード") or r.get("コード"))
        qty=num(r.get("quantity",r.get("保有数量",r.get("数量"))))
        if not sym or not re.fullmatch(r"[A-Z0-9.^=-]{1,20}",sym) or qty is None or qty<0:
            continue
        avg=num(r.get("average_cost",r.get("平均取得価額",r.get("平均取得単価",r.get("取得単価")))))
        result[sym]={
          "symbol":sym,"name":str(r.get("name") or r.get("銘柄名称") or r.get("銘柄名") or "")[:120],
          "quantity":qty,"average_cost":avg,
          "account_type":str(r.get("account_type") or r.get("口座区分") or "")[:40],
          "currency":str(r.get("currency") or "JPY")[:8]
        }
    if not result:
        raise ValueError("有効な保有明細がありません。既存データは変更していません")
    return list(result.values())

def sync_positions(rows, source, snapshot_at):
    items=normalize_positions(rows)
    dt=parse_dt(snapshot_at) if snapshot_at else now()
    canonical=json.dumps(items,ensure_ascii=False,sort_keys=True,separators=(",",":"))
    digest=hashlib.sha256((source+"|"+dt.isoformat()+"|"+canonical).encode()).hexdigest()
    with db() as c:
        if c.execute("SELECT 1 FROM sync_batches WHERE digest=?",(digest,)).fetchone():
            return {"duplicate":True,"imported":0,"rows":len(items)}
        prior=c.execute("SELECT snapshot_at FROM sync_batches WHERE source=? ORDER BY id DESC LIMIT 1",(source,)).fetchone()
        if prior and dt < parse_dt(prior["snapshot_at"]):
            raise ValueError("前回同期より古いスナップショットです。巻き戻しを防止しました")
        c.execute("INSERT INTO sync_batches(source,snapshot_at,digest,row_count,received_at) VALUES(?,?,?,?,?)",
                  (source,dt.isoformat(),digest,len(items),stamp()))
        c.execute("DELETE FROM holdings")
        for x in items:
            c.execute("""INSERT INTO holdings(symbol,name,quantity,average_cost,account_type,currency,source,updated_at)
              VALUES(?,?,?,?,?,?,?,?)""",(x["symbol"],x["name"],x["quantity"],x["average_cost"],x["account_type"],x["currency"],source,stamp()))
        c.execute("UPDATE settings SET v=? WHERE k='last_sync'",(stamp(),))
        c.execute("UPDATE settings SET v=? WHERE k='last_sync_source'",(source,))
    log_event("INFO",f"{len(items)}銘柄の保有スナップショットを同期: {source}")
    return {"duplicate":False,"imported":len(items),"rows":len(items)}

def telegram_send(text):
    tok=os.getenv("TELEGRAM_BOT_TOKEN","")
    chat=os.getenv("TELEGRAM_CHAT_ID","")
    if not tok or not chat:
        return False,"Telegram未設定"
    try:
        r=requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
          json={"chat_id":chat,"text":text[:3900],"disable_web_page_preview":True},timeout=15)
        r.raise_for_status()
        data=r.json()
        if not data.get("ok"):
            return False,str(data)[:300]
        return True,"sent"
    except Exception as e:
        log.exception("Telegram failure")
        return False,str(e)[:300]

def yahoo_history(sym):
    # Daily adjusted OHLCV; last completed daily candle is used.
    hist=yf.Ticker(sym).history(period="2y",interval="1d",auto_adjust=True, actions=False)
    if hist is None or hist.empty:
        raise ValueError("価格履歴なし")
    hist=hist.dropna(subset=["Close"])
    if len(hist)<220:
        raise ValueError(f"履歴不足（{len(hist)}営業日）。最低220日必要")
    hist=hist[~hist.index.duplicated(keep="last")]
    return hist

def technical_analysis(sym, hist):
    close=hist["Close"].astype(float)
    vol=hist["Volume"].astype(float) if "Volume" in hist else pd.Series(index=hist.index,dtype=float)
    px=float(close.iloc[-1])
    idx=hist.index[-1]
    mdate=idx.date().isoformat() if hasattr(idx,"date") else str(idx)[:10]
    asof=pd.Timestamp(idx).to_pydatetime()
    if asof.tzinfo is None:
        asof=asof.replace(tzinfo=timezone.utc)
    age=(now().date()-asof.astimezone(TZ).date()).days
    sma20=float(close.tail(20).mean()); sma50=float(close.tail(50).mean()); sma200=float(close.tail(200).mean())
    ema20=float(close.ewm(span=20,adjust=False).mean().iloc[-1])
    delta=close.diff()
    up=delta.clip(lower=0).ewm(alpha=1/14,adjust=False).mean()
    down=(-delta.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean()
    rs=up/down.replace(0,float("nan"))
    rsi=float((100-100/(1+rs)).iloc[-1])
    ret20=float(px/float(close.iloc[-21])-1)
    ret60=float(px/float(close.iloc[-61])-1)
    ret120=float(px/float(close.iloc[-121])-1)
    vol20=float(close.pct_change().tail(20).std()*math.sqrt(252))
    avgvol=float(vol.tail(20).mean()) if len(vol) else None
    lastvol=float(vol.iloc[-1]) if len(vol) else None
    # Relative volume is context only, not a standalone signal.
    relvol=(lastvol/avgvol) if avgvol and avgvol>0 and lastvol is not None else None
    score=50; reasons=[]
    tests=[
      (px>sma20,7,"終値 > SMA20","終値 ≤ SMA20"),
      (sma20>sma50,10,"SMA20 > SMA50","SMA20 ≤ SMA50"),
      (sma50>sma200,12,"SMA50 > SMA200","SMA50 ≤ SMA200"),
      (ret20>0,6,"20日リターンがプラス","20日リターンがマイナス"),
      (ret60>0,6,"60日リターンがプラス","60日リターンがマイナス"),
      (ret120>0,5,"120日リターンがプラス","120日リターンがマイナス"),
    ]
    for passed,weight,yes,no in tests:
        score += weight if passed else -weight
        reasons.append(yes if passed else no)
    if 45<=rsi<=68: score+=5; reasons.append(f"RSI {rsi:.1f}:中立〜強気")
    elif rsi>=78: score-=8; reasons.append(f"RSI {rsi:.1f}:過熱")
    elif rsi<30: reasons.append(f"RSI {rsi:.1f}:売られ過ぎ（買い根拠には単独使用しない）")
    if relvol and relvol>=1.5: reasons.append(f"出来高比 {relvol:.2f}倍")
    score=max(0,min(100,score))
    stale=age>PRICE_MAX_AGE_DAYS
    if stale:
        label="⚫【データ古い・判定停止】"
    elif px<sma20 and sma20<sma50 and sma50<sma200 and ret60<0 and ret120<0 and score<=20:
        label="🔵【売却再検討・要確認】"
    elif score>=78 and px>sma20>sma50>sma200 and ret20>0 and ret60>0:
        label="🟡【買い候補・要精査】"
    elif score<=35:
        label="🟠【警戒】"
    else:
        label="⚪【様子見】"
    return {
      "symbol":sym,"market_date":mdate,"price":round(px,4),"score":score,"label":label,
      "sma20":round(sma20,4),"sma50":round(sma50,4),"sma200":round(sma200,4),
      "ema20":round(ema20,4),"rsi14":round(rsi,2),"return20_pct":round(ret20*100,2),
      "return60_pct":round(ret60*100,2),"return120_pct":round(ret120*100,2),
      "annualized_volatility":round(vol20,3),"relative_volume":round(relvol,2) if relvol else None,
      "data_age_days":age,"asof":asof.isoformat(),"source":"Yahoo Finance / yfinance",
      "reasons":reasons,"data_warning":"Yahoo Financeは遅延・欠損・訂正の可能性。発注前に楽天証券画面と一次資料で照合。"
    }

def save_analysis(a):
    with db() as c:
        c.execute("""INSERT OR IGNORE INTO analyses(symbol,market_date,score,label,price,details,created_at)
          VALUES(?,?,?,?,?,?,?)""",(a["symbol"],a["market_date"],a["score"],a["label"],a["price"],json.dumps(a,ensure_ascii=False),stamp()))
        c.execute("""INSERT OR IGNORE INTO daily_prices(symbol,market_date,close,volume,source,fetched_at)
          VALUES(?,?,?,?,?,?)""",(a["symbol"],a["market_date"],a["price"],None,a["source"],stamp()))

def position_context(sym, price):
    with db() as c:
        p=c.execute("SELECT * FROM holdings WHERE symbol=?",(sym,)).fetchone()
    if not p:
        return "保有数量未同期。株数・売買金額の提案は保留。"
    q=float(p["quantity"]); avg=p["average_cost"]
    pnl=None if avg is None else (price/avg-1)*100
    budget=num(get_setting("alert_budget_jpy","30000")) or 0
    estimated=int(budget//price) if price>0 and budget>0 else 0
    amt=estimated*price
    return (f"保有 {q:g}株 / 平均取得 {avg:,.2f}円" if avg is not None else f"保有 {q:g}株 / 平均取得単価未登録") + \
      (f" / 取得比 {pnl:+.1f}%" if pnl is not None else "") + \
      f" / 設定予算内の単純試算 {estimated}株・約{amt:,.0f}円（現金余力・注文単位・NISA枠未反映）"

def confirmation_and_alert(a):
    label=a["label"]; sym=a["symbol"]; mdate=a["market_date"]
    if label in ("⚪【様子見】","⚫【データ古い・判定停止】"):
        return None
    with db() as c:
        prev=c.execute("SELECT * FROM signal_state WHERE symbol=?",(sym,)).fetchone()
        if prev and prev["label"]==label:
            count=int(prev["confirmations"])+ (0 if prev["last_seen"]==mdate else 1)
            first=prev["first_seen"]
            last_alert=prev["last_alert"]
        else:
            count=1; first=mdate; last_alert=None
        c.execute("""INSERT INTO signal_state(symbol,label,first_seen,last_seen,confirmations,last_alert)
          VALUES(?,?,?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET label=excluded.label,
          first_seen=excluded.first_seen,last_seen=excluded.last_seen,confirmations=excluded.confirmations,
          last_alert=CASE WHEN excluded.label<>signal_state.label THEN NULL ELSE signal_state.last_alert END""",
          (sym,label,first,mdate,count,last_alert))
        # Two separate market sessions required. No repeated alert within cooldown.
        if count<2:
            return None
        if last_alert:
            try:
                if (now()-parse_dt(last_alert)).days<SIGNAL_COOLDOWN_DAYS:
                    return None
            except Exception:
                pass
        key=c.execute("SELECT 1 FROM alerts WHERE symbol=? AND label=? AND market_date=?",(sym,label,mdate)).fetchone()
        if key:
            return None
        msg=(f"{label}\n{sym} | スコア {a['score']}/100 | {a['price']:,.2f}円\n"
             f"確認: 別々の市場日で2回\n根拠: "+" / ".join(a["reasons"])+"\n"+
             position_context(sym,a["price"])+f"\n価格データ日: {mdate}\n"+
             "売買指示ではありません。決算・適時開示・資金余力・注文条件を確認してください。")
        c.execute("INSERT INTO alerts(symbol,label,market_date,score,message,created_at) VALUES(?,?,?,?,?,?)",
                  (sym,label,mdate,a["score"],msg,stamp()))
        c.execute("UPDATE signal_state SET last_alert=? WHERE symbol=?",(stamp(),sym))
        return msg

def fetch_news(sym):
    # Google News RSS query is discovery-only; never used alone to trigger trade labels.
    q=urllib.parse.quote(f'"{sym.replace(".T","")}" stock Japan')
    url=f"https://news.google.com/rss/search?q={q}&hl=ja&gl=JP&ceid=JP:ja"
    req=urllib.request.Request(url,headers={"User-Agent":"InvestmentWatchdog/2.0 contact: operator"})
    try:
        with urllib.request.urlopen(req,timeout=10) as resp:
            raw=resp.read(1_000_000)
        root=ET.fromstring(raw)
        found=[]
        for item in root.findall(".//item")[:12]:
            title=(item.findtext("title") or "").strip()
            link=(item.findtext("link") or "").strip()
            pub=(item.findtext("pubDate") or "").strip()
            source=(item.findtext("source") or "Google News RSS").strip()
            if not title or not link: continue
            digest=hashlib.sha256((sym+title+link).encode()).hexdigest()
            found.append((sym,title[:500],link[:1500],pub[:100],source[:120],digest,stamp()))
        with db() as c:
            c.executemany("""INSERT OR IGNORE INTO news(symbol,title,link,published,source,digest,fetched_at)
              VALUES(?,?,?,?,?,?,?)""",found)
        return len(found)
    except Exception as e:
        log.warning("news fetch %s failed: %s",sym,e)
        return 0

def scan_all():
    started=stamp()
    with db() as c:
        c.execute("""INSERT INTO jobs(name,last_started,status,detail) VALUES('daily_scan',?,'running','')
          ON CONFLICT(name) DO UPDATE SET last_started=excluded.last_started,status='running'""",(started,))
    symbols=[symbol_norm(x) for x in get_setting("watchlist",WATCHLIST_DEFAULT).split(",") if x.strip()]
    symbols=list(dict.fromkeys(symbols))[:MAX_SYMBOLS]
    good=bad=sent=0
    for sym in symbols:
        try:
            hist=yahoo_history(sym)
            a=technical_analysis(sym,hist)
            save_analysis(a); good+=1
            msg=confirmation_and_alert(a)
            fetch_news(sym)
            if msg:
                ok,err=telegram_send(msg)
                with db() as c:
                    c.execute("UPDATE alerts SET sent=?,send_error=? WHERE symbol=? AND label=? AND market_date=?",
                              (1 if ok else 0, None if ok else err,sym,a["label"],a["market_date"]))
                if ok: sent+=1
                else: log_event("ERROR",f"Telegram送信失敗 {sym}: {err}")
        except Exception as e:
            bad+=1; log.exception("scan failed %s",sym); log_event("WARNING",f"{sym}: {str(e)[:300]}")
    detail=f"成功{good} / 失敗{bad} / 通知送信{sent}"
    put_setting("last_scan",stamp()); put_setting("last_scan_status",detail)
    with db() as c:
        c.execute("UPDATE jobs SET last_finished=?,status=?,detail=? WHERE name='daily_scan'",(stamp(),"ok" if bad==0 else "partial",detail))
    log_event("INFO" if bad==0 else "WARNING",detail)
    return {"success":good,"failed":bad,"sent":sent,"detail":detail}

def send_daily_report():
    with db() as c:
        latest=c.execute("""SELECT a.* FROM analyses a JOIN
          (SELECT symbol,MAX(market_date) d FROM analyses GROUP BY symbol) x
          ON a.symbol=x.symbol AND a.market_date=x.d ORDER BY a.score DESC""").fetchall()
        n=c.execute("SELECT COUNT(*) n FROM news WHERE fetched_at>=?",((now()-timedelta(days=1)).isoformat(),)).fetchone()["n"]
    if not latest: return
    lines=["Investment Watchdog 日次レポート",f"分析銘柄: {len(latest)} | 新着ニュース候補: {n}"]
    for r in latest[:12]:
        lines.append(f"{r['symbol']} {r['label']} score={r['score']} price={r['price']:,.2f}")
    lines.append("ニュースは見出し候補です。決算/適時開示の一次資料を必ず確認。")
    telegram_send("\n".join(lines))

def holdings_list():
    with db() as c:
        return [dict(x) for x in c.execute("SELECT * FROM holdings ORDER BY symbol")]

@app.get("/health")
@app.get("/api/health")
def health():
    try:
        with db() as c:
            c.execute("SELECT 1")
            job=c.execute("SELECT * FROM jobs WHERE name='daily_scan'").fetchone()
        return jsonify(status="ok",version=APP_VERSION,time=stamp(),
          database="ok",last_scan=get_setting("last_scan"),last_scan_status=get_setting("last_scan_status"),
          scheduler=bool(scheduler and scheduler.running),
          last_job=dict(job) if job else None,
          sync_configured=bool(os.getenv("SYNC_API_TOKEN")),
          telegram_configured=bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID")))
    except Exception as e:
        return jsonify(status="error",error=str(e)[:200]),500

@app.get("/")
@require_dashboard
def dashboard():
    with db() as c:
        analyses=[json.loads(r["details"]) for r in c.execute("""SELECT a.details FROM analyses a
          JOIN (SELECT symbol,MAX(market_date) d FROM analyses GROUP BY symbol) x
          ON a.symbol=x.symbol AND a.market_date=x.d ORDER BY a.score DESC""")]
        news=[dict(r) for r in c.execute("SELECT * FROM news ORDER BY id DESC LIMIT 25")]
        alerts=[dict(r) for r in c.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT 20")]
        events=[dict(r) for r in c.execute("SELECT * FROM events ORDER BY id DESC LIMIT 15")]
    return render_template_string(HTML,analyses=analyses,holdings=holdings_list(),news=news,
      alerts=alerts,events=events,last_scan=get_setting("last_scan"),scan_status=get_setting("last_scan_status"),
      last_sync=get_setting("last_sync"),sync_source=get_setting("last_sync_source"),
      watchlist=get_setting("watchlist",WATCHLIST_DEFAULT),budget=get_setting("alert_budget_jpy","30000"),
      maxpct=get_setting("max_position_pct","15"),telegram=bool(os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID")))

@app.post("/settings")
@require_dashboard
def settings():
    wl=request.form.get("watchlist","").strip()
    syms=[symbol_norm(x) for x in wl.split(",") if x.strip()]
    if not syms or len(syms)>MAX_SYMBOLS or any(not re.fullmatch(r"[A-Z0-9.^=-]{1,20}",x) for x in syms):
        flash("監視銘柄の形式が不正です"); return redirect(url_for("dashboard"))
    budget=num(request.form.get("budget")); maxpct=num(request.form.get("maxpct"))
    if budget is None or budget<0 or maxpct is None or not 1<=maxpct<=100:
        flash("予算または上限比率が不正です"); return redirect(url_for("dashboard"))
    put_setting("watchlist",",".join(syms)); put_setting("alert_budget_jpy",budget); put_setting("max_position_pct",maxpct)
    flash("設定を保存しました"); return redirect(url_for("dashboard"))

@app.post("/scan")
@require_dashboard
def manual_scan():
    result=scan_all(); flash(result["detail"]); return redirect(url_for("dashboard"))

@app.post("/telegram-test")
@require_dashboard
def telegram_test():
    ok,msg=telegram_send("Investment Watchdog: Telegram接続テスト")
    flash("Telegram送信成功" if ok else "Telegram送信失敗: "+msg)
    return redirect(url_for("dashboard"))

@app.post("/sync/rakuten/rss")
@require_sync
def rakuten_sync():
    if request.content_length and request.content_length>MAX_BODY: return jsonify(error="payload too large"),413
    data=request.get_json(silent=True) or {}
    try:
        out=sync_positions(data.get("positions"),"rakuten_marketspeed2_rss",data.get("snapshot_at"))
        return jsonify(ok=True,**out,received_at=stamp())
    except ValueError as e:
        return jsonify(error=str(e)),422

@app.post("/sync/positions")
@require_sync
def generic_sync():
    if request.content_length and request.content_length>MAX_BODY: return jsonify(error="payload too large"),413
    data=request.get_json(silent=True) or {}
    try:
        out=sync_positions(data.get("positions"),"authenticated_api",data.get("snapshot_at"))
        return jsonify(ok=True,**out)
    except ValueError as e:
        return jsonify(error=str(e)),422

@app.post("/import/csv")
@require_sync
def import_csv():
    f=request.files.get("file")
    if not f: return jsonify(error="file field required"),400
    raw=f.read(MAX_BODY+1)
    if len(raw)>MAX_BODY: return jsonify(error="file too large"),413
    try:
        rows=list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
        out=sync_positions(rows,"csv_import",request.form.get("snapshot_at"))
        return jsonify(ok=True,**out)
    except Exception as e:
        return jsonify(error=str(e)[:300]),422

@app.get("/api/holdings")
@require_sync
def api_holdings():
    return jsonify(holdings=holdings_list(),last_sync=get_setting("last_sync"),source=get_setting("last_sync_source"))

@app.get("/api/analysis")
@require_sync
def api_analysis():
    with db() as c:
        rows=[json.loads(r["details"]) for r in c.execute("""SELECT a.details FROM analyses a JOIN
          (SELECT symbol,MAX(market_date) d FROM analyses GROUP BY symbol) x
          ON a.symbol=x.symbol AND a.market_date=x.d""")]
    return jsonify(analysis=rows)

@app.get("/api/news")
@require_sync
def api_news():
    with db() as c:
        rows=[dict(r) for r in c.execute("SELECT symbol,title,link,published,source,fetched_at FROM news ORDER BY id DESC LIMIT 200")]
    return jsonify(news=rows)

@app.get("/api/alerts")
@require_sync
def api_alerts():
    with db() as c:
        rows=[dict(r) for r in c.execute("SELECT * FROM alerts ORDER BY id DESC LIMIT 200")]
    return jsonify(alerts=rows)

@app.get("/api/backtest/<path:sym>")
@require_sync
def backtest(sym):
    try:
        h=yahoo_history(symbol_norm(sym))
        close=h["Close"].astype(float)
        fast=close.rolling(20).mean(); slow=close.rolling(50).mean()
        pos=(fast>slow).astype(float).shift(1).fillna(0)
        ret=close.pct_change().fillna(0)
        # Illustrative round-trip friction estimate; adjust before relying on results.
        turnover=pos.diff().abs().fillna(pos.iloc[0])
        strat=pos*ret-turnover*0.0015
        bench=ret
        def stats(r):
            eq=(1+r).cumprod(); years=max(len(r)/252,0.1); dd=eq/eq.cummax()-1
            return {"total_return_pct":round((eq.iloc[-1]-1)*100,2),
              "annualized_return_pct":round((eq.iloc[-1]**(1/years)-1)*100,2),
              "max_drawdown_pct":round(dd.min()*100,2)}
        return jsonify(symbol=symbol_norm(sym),period=f"{h.index[0]} to {h.index[-1]}",
          sma20_50=stats(strat),buy_hold=stats(bench),
          caveats=["簡易参考検証。将来予測ではありません","0.15%の売買摩擦を仮置き。実際の手数料/スプレッド/税/配当再投資/約定遅延を完全再現しません",
          "パラメータ最適化・ウォークフォワード・アウトオブサンプル検証・上場廃止銘柄を含む生存者バイアス補正は未実装"])
    except Exception as e: return jsonify(error=str(e)[:300]),503

HTML=r"""<!doctype html><html lang=ja><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Investment Watchdog</title>
<style>body{margin:0;padding:18px;background:#0b1220;color:#e9eef6;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1200px;margin:auto}h1{font-size:25px}.muted,small{color:#9baac0}.card{background:#141f31;border:1px solid #2c3c53;border-radius:12px;padding:15px;margin:12px 0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:10px}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-size:13px}td,th{text-align:left;padding:8px;border-bottom:1px solid #2b3b50;vertical-align:top}th{color:#aab7ca}input{box-sizing:border-box;width:100%;padding:10px;background:#0b1220;color:white;border:1px solid #43546c;border-radius:7px;margin:5px 0 12px}button{padding:10px 14px;background:#315d9b;color:white;border:0;border-radius:8px;margin:4px}a{color:#8dbbff}.warn{color:#ffcc7a}.ok{color:#8ee0b4}</style></head><body><main>
<h1>Investment Watchdog <small>v2.0</small></h1><p class=muted>分析支援ツール｜自動発注なし｜シグナルは売買指示ではありません</p>
<div class=grid><div class=card><small>最終スキャン</small><h3>{{last_scan or "未実行"}}</h3>{{scan_status}}</div><div class=card><small>楽天RSS同期</small><h3>{{last_sync or "未同期"}}</h3>{{sync_source or "—"}}</div><div class=card><small>Telegram</small><h3>{{"設定済み" if telegram else "未設定"}}</h3><form method=post action=/telegram-test><button>接続テスト</button></form></div></div>
{% with messages=get_flashed_messages() %}{% for m in messages %}<div class=card>{{m}}</div>{% endfor %}{% endwith %}
<div class=card><h2>監視設定</h2><form method=post action=/settings><label>監視銘柄（カンマ区切り）</label><input name=watchlist value="{{watchlist}}"><div class=grid><div><label>通知時の参考予算（円）</label><input name=budget type=number min=0 value="{{budget}}"></div><div><label>最大保有比率の参考上限（%）</label><input name=maxpct type=number min=1 max=100 value="{{maxpct}}"></div></div><button>保存</button></form><form method=post action=/scan><button>今すぐ分析</button></form></div>
<div class=card><h2>保有資産</h2><div class=scroll><table><tr><th>コード</th><th>銘柄</th><th>数量</th><th>平均取得</th><th>口座</th><th>同期元</th></tr>{% for h in holdings %}<tr><td>{{h.symbol}}</td><td>{{h.name}}</td><td>{{h.quantity}}</td><td>{{h.average_cost or "未登録"}}</td><td>{{h.account_type}}</td><td>{{h.source}}</td></tr>{% else %}<tr><td colspan=6>未同期</td></tr>{% endfor %}</table></div></div>
<div class=card><h2>最新分析</h2><div class=scroll><table><tr><th>銘柄</th><th>判定</th><th>スコア</th><th>株価</th><th>RSI</th><th>20日</th><th>60日</th><th>データ日</th><th>根拠</th></tr>{% for a in analyses %}<tr><td>{{a.symbol}}</td><td>{{a.label}}</td><td>{{a.score}}</td><td>{{a.price}}</td><td>{{a.rsi14}}</td><td>{{a.return20_pct}}%</td><td>{{a.return60_pct}}%</td><td>{{a.market_date}}</td><td>{{a.reasons|join(" / ")}}</td></tr>{% else %}<tr><td colspan=9>分析データなし</td></tr>{% endfor %}</table></div></div>
<div class=card><h2>通知履歴</h2><div class=scroll><table><tr><th>日時</th><th>銘柄</th><th>判定</th><th>スコア</th><th>送信</th><th>内容</th></tr>{% for a in alerts %}<tr><td>{{a.created_at}}</td><td>{{a.symbol}}</td><td>{{a.label}}</td><td>{{a.score}}</td><td>{{"成功" if a.sent else "未送信/失敗"}}</td><td>{{a.message}}</td></tr>{% else %}<tr><td colspan=6>通知なし</td></tr>{% endfor %}</table></div></div>
<div class=card><h2>ニュース見出し候補</h2>{% for n in news %}<p><b>{{n.symbol}}</b> <a href="{{n.link}}" target=_blank rel=noopener>{{n.title}}</a><br><small>{{n.published}} / {{n.source}}</small></p>{% else %}<p>なし</p>{% endfor %}<p class=warn>ニュースRSSは検索用候補。決算・適時開示の一次資料を確認し、見出しだけで売買しないこと。</p></div>
<div class=card><h2>稼働ログ</h2>{% for e in events %}<p><small>{{e.created_at}} [{{e.level}}]</small> {{e.message}}</p>{% else %}<p>ログなし</p>{% endfor %}</div>
<p class=muted>価格データ: Yahoo Finance。更新遅延・欠損・誤差の可能性があります。日本株のリアルタイム価格保証なし。</p></main></body></html>"""

def start_scheduler():
    global scheduler
    if os.getenv("ENABLE_SCHEDULER","1")!="1": return
    with scheduler_lock:
        if scheduler and scheduler.running: return
        scheduler=BackgroundScheduler(timezone=TZ)
        scheduler.add_job(scan_all,CronTrigger(day_of_week="mon-fri",hour=SCAN_HOUR,minute=SCAN_MINUTE,timezone=TZ),
          id="weekday_scan",max_instances=1,coalesce=True,misfire_grace_time=3600)
        scheduler.add_job(send_daily_report,CronTrigger(day_of_week="mon-fri",hour=SCAN_HOUR,minute=45,timezone=TZ),
          id="daily_report",max_instances=1,coalesce=True,misfire_grace_time=3600)
        scheduler.start()

init_db()
start_scheduler()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","10000")),debug=False)
