"""
Investment Watchdog - app.py
Python 3.11+
Run:
  pip install -r requirements.txt
  python app.py

Important deployment notes:
- Run exactly ONE scheduler process (RUN_SCHEDULER=true). For Render, use one
  Background Worker for this file, or one web instance with one worker process.
- ChatGPT's own push-notification system cannot be enabled by this Flask app.
  This app sends Telegram and/or generic webhook notifications when configured.
- Signals are rule-based screening alerts, not individualized financial advice.
  No brokerage connection or automatic order placement is implemented.
"""

import os
import time
import json
import sqlite3
import logging
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from threading import Lock

import requests
import yfinance as yf
from flask import Flask, jsonify, render_template_string, request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# --------------------------- Configuration ---------------------------

JST = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Tokyo"))
DB_PATH = os.getenv("DB_PATH", "watchdog.db")
PORT = int(os.getenv("PORT", "10000"))
RUN_SCHEDULER = os.getenv("RUN_SCHEDULER", "true").lower() == "true"

CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "600"))
ALERT_BUDGET_YEN = float(os.getenv("ALERT_BUDGET_YEN", "0"))
MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", "0.10"))
MIN_ORDER_YEN = float(os.getenv("MIN_ORDER_YEN", "5000"))
CASH_AVAILABLE_YEN = float(os.getenv("CASH_AVAILABLE_YEN", "0"))
PORTFOLIO_VALUE_YEN = float(os.getenv("PORTFOLIO_VALUE_YEN", "0"))
ALERT_COOLDOWN_HOURS = int(os.getenv("ALERT_COOLDOWN_HOURS", "8"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
WEBHOOK_BEARER_TOKEN = os.getenv("WEBHOOK_BEARER_TOKEN", "")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Keep holdings synchronized with the user's latest confirmation.
# Update this list after every trade. BTC quantity remains unknown, so only
# BTC market price/technical trend is monitored; no BTC unit count is assumed.
HOLDINGS = [
    {"name": "日東紡績", "ticker": "3110.T", "shares": 60, "kind": "stock"},
    {"name": "キオクシア", "ticker": "285A.T", "shares": 15, "kind": "stock"},
    {"name": "指月電機製作所", "ticker": "6994.T", "shares": 100, "kind": "stock"},
    {"name": "ソフトバンク", "ticker": "9434.T", "shares": 200, "kind": "stock"},
    {"name": "ソフトバンクグループ", "ticker": "9984.T", "shares": 45, "kind": "stock"},
    {"name": "東洋エンジニアリング", "ticker": "6330.T", "shares": 22, "kind": "stock"},
    {"name": "S&P500半導体ETF", "ticker": "346A.T", "shares": 55, "kind": "stock"},
    {"name": "ビットコイン", "ticker": "BTC-JPY", "shares": None, "kind": "crypto"},
]

SIGNALS = {
    "BUY_NOW": "🔴【即時買い】",
    "CHANCE": "🟡【チャンス】",
    "WARNING": "🟠【注意】",
    "SELL_NOW": "🔵【即時売り】",
    "HOLD": "⚪【様子見】",
    "ERROR": "⚫【データ未取得】",
}

app = Flask(__name__)
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("investment-watchdog")

started_at = time.time()
scheduler = None
cache_lock = Lock()
cache = {"at": 0.0, "payload": None}
last_notification_fingerprints = {}


# --------------------------- Database ---------------------------

def connect_db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def now_jst():
    return datetime.now(JST).isoformat(timespec="seconds")


def init_db():
    with connect_db() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                task TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS notification_state (
                fingerprint TEXT PRIMARY KEY,
                last_sent_at TEXT NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at)")


def log_run(task, status, detail=""):
    with connect_db() as con:
        con.execute(
            "INSERT INTO runs(created_at,task,status,detail) VALUES(?,?,?,?)",
            (now_jst(), task, status, str(detail)[:2000]),
        )


def record_event(category, title, body):
    with connect_db() as con:
        con.execute(
            "INSERT INTO events(created_at,category,title,body) VALUES(?,?,?,?)",
            (now_jst(), category, title, body[:5000]),
        )


# --------------------------- Notifications ---------------------------

def send_notification(message):
    """Send through configured external channels. Returns successful channel names."""
    sent = []
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": message[:4000]},
                timeout=15,
            )
            r.raise_for_status()
            if not r.json().get("ok", False):
                raise RuntimeError("Telegram API returned ok=false")
            sent.append("telegram")
        except Exception:
            log.exception("Telegram notification failed")

    if WEBHOOK_URL:
        try:
            headers = {"Content-Type": "application/json"}
            if WEBHOOK_BEARER_TOKEN:
                headers["Authorization"] = f"Bearer {WEBHOOK_BEARER_TOKEN}"
            r = requests.post(
                WEBHOOK_URL,
                json={"text": message[:10000], "message": message[:10000]},
                headers=headers,
                timeout=15,
            )
            r.raise_for_status()
            sent.append("webhook")
        except Exception:
            log.exception("Webhook notification failed")
    return sent


def send_deduplicated(key, message, cooldown_hours=None):
    """Avoid repeatedly sending the same alert during the cooldown window."""
    cooldown = cooldown_hours or ALERT_COOLDOWN_HOURS
    fingerprint = hashlib.sha256(key.encode("utf-8")).hexdigest()
    now = datetime.now(JST)
    with connect_db() as con:
        row = con.execute(
            "SELECT last_sent_at FROM notification_state WHERE fingerprint=?",
            (fingerprint,),
        ).fetchone()
        if row:
            try:
                previous = datetime.fromisoformat(row["last_sent_at"])
                if now - previous < timedelta(hours=cooldown):
                    return []
            except ValueError:
                pass
        con.execute("""
            INSERT INTO notification_state(fingerprint,last_sent_at) VALUES(?,?)
            ON CONFLICT(fingerprint) DO UPDATE SET last_sent_at=excluded.last_sent_at
        """, (fingerprint, now.isoformat(timespec="seconds")))
    return send_notification(message)


# --------------------------- Market data and signals ---------------------------

def compute_rsi(close, period=14):
    delta = close.diff()
    gains = delta.clip(lower=0).rolling(period).mean()
    losses = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gains / losses.replace(0, float("nan"))
    result = 100 - (100 / (1 + rs))
    if result.empty or result.iloc[-1] != result.iloc[-1]:
        return None
    return float(result.iloc[-1])


def build_order_guide(item, price, signal_code):
    """
    Calculates a conservative indicative budget only.
    It does NOT account for NISA availability, fees, liquidity, tax, or live order book.
    """
    if item["kind"] != "stock" or not price or price <= 0:
        return {"budget_yen": None, "shares": None, "note": "株式の買付目安対象外"}

    if signal_code not in ("CHANCE", "BUY_NOW"):
        return {"budget_yen": 0, "shares": 0, "note": "買付シグナルなし"}

    budget = ALERT_BUDGET_YEN
    if PORTFOLIO_VALUE_YEN > 0:
        budget = min(budget, PORTFOLIO_VALUE_YEN * MAX_POSITION_PCT) if budget > 0 else PORTFOLIO_VALUE_YEN * MAX_POSITION_PCT
    if CASH_AVAILABLE_YEN > 0:
        budget = min(budget, CASH_AVAILABLE_YEN) if budget > 0 else CASH_AVAILABLE_YEN

    if budget <= 0:
        return {
            "budget_yen": None, "shares": None,
            "note": "予算未設定。ALERT_BUDGET_YEN等を設定すると算出",
        }

    whole_shares = int(budget // price)
    if whole_shares <= 0 or budget < MIN_ORDER_YEN:
        return {"budget_yen": 0, "shares": 0, "note": "設定予算では単元・最低金額に届かない可能性"}
    return {
        "budget_yen": int(whole_shares * price),
        "shares": whole_shares,
        "note": "参考計算。発注前に株価・単元株・余力を再確認",
    }


def classify_signal(price, sma25, sma75, rsi, previous_close=None):
    """
    Transparent screening rules:
    - BUY_NOW: bullish trend + positive momentum + RSI not overheated
    - CHANCE: bullish trend or oversold bounce setup (confirmation required)
    - SELL_NOW: bearish trend + weak momentum
    - WARNING: overbought or trend deterioration
    These thresholds are configurable only by editing this function.
    """
    if sma75 is None or rsi is None:
        return "HOLD", "履歴不足のため強い判定をしない"

    bullish = price > sma25 > sma75
    bearish = price < sma25 < sma75

    if bullish and 50 <= rsi <= 65:
        return "BUY_NOW", "株価が25日線・75日線の上で推移し、RSIも過熱圏ではない"
    if bearish and rsi < 40:
        return "SELL_NOW", "株価が25日線・75日線を下回り、RSIも弱い"
    if bullish and 45 <= rsi < 50:
        return "CHANCE", "上昇トレンドを維持。押し目候補だが反発確認が必要"
    if rsi >= 75:
        return "WARNING", "RSIが75以上。短期的な過熱に注意"
    if price < sma25:
        return "WARNING", "株価が25日線を下回っている"
    if rsi <= 30 and price > sma75:
        return "CHANCE", "RSIが低水準。反発確認が必要"
    return "HOLD", "明確な売買条件なし"


def fetch_one(item):
    try:
        ticker = yf.Ticker(item["ticker"])
        hist = ticker.history(period="1y", interval="1d", auto_adjust=True)
        if hist is None or hist.empty:
            raise ValueError("価格履歴なし")
        close = hist["Close"].dropna()
        if len(close) < 80:
            raise ValueError(f"分析可能な履歴が不足（{len(close)}営業日）")

        price = float(close.iloc[-1])
        previous_close = float(close.iloc[-2]) if len(close) >= 2 else None
        sma25 = float(close.rolling(25).mean().iloc[-1])
        sma75 = float(close.rolling(75).mean().iloc[-1])
        rsi = compute_rsi(close, 14)
        code, reason = classify_signal(price, sma25, sma75, rsi, previous_close)
        guide = build_order_guide(item, price, code)

        return {
            **item,
            "price": round(price, 3),
            "previous_close": round(previous_close, 3) if previous_close else None,
            "change_pct": round((price / previous_close - 1) * 100, 2) if previous_close else None,
            "sma25": round(sma25, 3),
            "sma75": round(sma75, 3),
            "rsi14": round(rsi, 1) if rsi is not None else None,
            "signal_code": code,
            "signal": SIGNALS[code],
            "reason": reason,
            "order_guide": guide,
            "data_date": str(close.index[-1]),
            "error": None,
        }
    except Exception as exc:
        log.exception("Data fetch failed for %s", item["ticker"])
        return {
            **item, "price": None, "previous_close": None, "change_pct": None,
            "sma25": None, "sma75": None, "rsi14": None,
            "signal_code": "ERROR", "signal": SIGNALS["ERROR"],
            "reason": str(exc)[:200], "order_guide": None,
            "data_date": None, "error": str(exc)[:300],
        }


def collect(force=False):
    with cache_lock:
        if not force and cache["payload"] and time.time() - cache["at"] < CACHE_SECONDS:
            return cache["payload"]

    items = [fetch_one(item) for item in HOLDINGS]
    payload = {
        "service": "investment-watchdog",
        "status": "running",
        "updated_at": now_jst(),
        "items": items,
        "data_source": "Yahoo Finance via yfinance",
        "disclaimer": "データ遅延・欠損の可能性あり。シグナルは参考情報で、売買の保証ではありません。",
    }
    with cache_lock:
        cache["at"] = time.time()
        cache["payload"] = payload
    return payload


# --------------------------- Scheduled jobs ---------------------------

def portfolio_monitor():
    task = "hourly_portfolio_monitor"
    try:
        data = collect(force=True)
        important = [
            x for x in data["items"]
            if x["signal_code"] in ("BUY_NOW", "CHANCE", "WARNING", "SELL_NOW", "ERROR")
        ]
        if important:
            lines = []
            for x in important:
                guide = x.get("order_guide") or {}
                amount = guide.get("budget_yen")
                shares = guide.get("shares")
                order_text = (
                    f" / 買付目安 {amount:,}円・{shares}株"
                    if amount is not None and shares is not None and shares > 0
                    else ""
                )
                price_text = f'{x["price"]:,}' if x["price"] is not None else "取得不可"
                lines.append(
                    f'{x["signal"]} {x["name"]}: {price_text}円 / '
                    f'RSI {x["rsi14"]} / {x["reason"]}{order_text}'
                )
            body = "【投資監視】\n" + "\n".join(lines)
            record_event("monitor", "定期投資監視", body)
            for x in important:
                key = f'{x["ticker"]}:{x["signal_code"]}:{x["reason"]}'
                send_deduplicated(key, body)
        else:
            record_event("monitor", "定期投資監視", "通知対象なし。全銘柄を確認しました。")
        log_run(task, "success", f'{len(data["items"])}銘柄確認')
    except Exception as exc:
        log.exception("Portfolio monitor failed")
        log_run(task, "failed", exc)
        send_deduplicated(f"{task}:failure", f"⚠️ 投資監視に失敗しました。\n{str(exc)[:500]}", 1)


def market_report():
    task = "market_report"
    try:
        data = collect(force=True)
        lines = []
        for x in data["items"]:
            price = f'{x["price"]:,.0f}円' if x["price"] is not None else "価格取得不可"
            lines.append(f'{x["name"]}: {price} / {x["signal"]}')
        body = "【定時市場レポート】\n" + "\n".join(lines) + f'\n確認時刻: {now_jst()}'
        record_event("report", "市場・保有銘柄レポート", body)
        sent = send_notification(body)
        log_run(task, "success", f"channels={sent}")
    except Exception as exc:
        log.exception("Market report failed")
        log_run(task, "failed", exc)
        send_deduplicated(f"{task}:failure", f"⚠️ 定時レポート失敗: {str(exc)[:500]}", 1)


def health_audit():
    task = "hourly_health_audit"
    try:
        since = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        with connect_db() as con:
            failures = con.execute(
                "SELECT COUNT(*) FROM runs WHERE status='failed' AND created_at >= ?",
                (since,),
            ).fetchone()[0]
            last_monitor = con.execute(
                "SELECT created_at,status FROM runs WHERE task='hourly_portfolio_monitor' ORDER BY id DESC LIMIT 1"
            ).fetchone()

        stale_minutes = None
        if last_monitor:
            try:
                last_time = datetime.fromisoformat(last_monitor["created_at"])
                stale_minutes = int((datetime.now(JST) - last_time).total_seconds() / 60)
            except ValueError:
                pass

        problems = []
        if failures >= 3:
            problems.append(f"本日の失敗回数が{failures}回")
        if last_monitor is None:
            problems.append("投資監視の実行記録なし")
        elif stale_minutes is not None and stale_minutes > 90:
            problems.append(f"最終監視から{stale_minutes}分経過")

        status = "warning" if problems else "success"
        detail = f"today_failed_runs={failures}; last_monitor_age_min={stale_minutes}"
        log_run(task, status, detail)

        if problems:
            message = "🟠【稼働点検・要確認】\n" + "\n".join(problems) + "\nログを確認してください。"
            record_event("health", "稼働点検警告", message)
            send_deduplicated(f"health:{'|'.join(problems)}", message, 2)
    except Exception as exc:
        log.exception("Health audit failed")
        log_run(task, "failed", exc)


def routine_status_notice():
    task = "routine_status_notice"
    try:
        with connect_db() as con:
            rows = con.execute(
                "SELECT task,status,created_at,detail FROM runs ORDER BY id DESC LIMIT 10"
            ).fetchall()
        if not rows:
            body = "【稼働点検】まだ実行履歴がありません。"
        else:
            lines = [f'{r["task"]}: {r["status"]} ({r["created_at"]})' for r in rows]
            body = "【稼働点検・簡潔版】\n" + "\n".join(lines)
        record_event("health", "定時稼働報告", body)
        sent = send_notification(body)
        log_run(task, "success", f"channels={sent}")
    except Exception as exc:
        log.exception("Routine status notice failed")
        log_run(task, "failed", exc)


def start_scheduler():
    global scheduler
    if not RUN_SCHEDULER:
        log.info("Scheduler disabled by RUN_SCHEDULER")
        return
    if scheduler and scheduler.running:
        return

    scheduler = BackgroundScheduler(timezone=JST)
    scheduler.add_job(
        portfolio_monitor, CronTrigger(minute=0), id="hourly_portfolio",
        replace_existing=True, max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        health_audit, CronTrigger(minute=10), id="hourly_health",
        replace_existing=True, max_instances=1, coalesce=True,
    )
    for hour in (8, 16, 20):
        scheduler.add_job(
            market_report, CronTrigger(hour=hour, minute=30),
            id=f"market_report_{hour}", replace_existing=True,
            max_instances=1, coalesce=True,
        )
    scheduler.add_job(
        routine_status_notice, CronTrigger(hour="9,21", minute=0),
        id="status_notice", replace_existing=True, max_instances=1, coalesce=True,
    )
    scheduler.start()
    log.info("Scheduler started in %s", JST)


# --------------------------- Web interface ---------------------------

PAGE = r"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Investment Watchdog</title>
<style>
body{margin:0;padding:16px;background:#10151d;color:#eef2f7;font-family:-apple-system,BlinkMacSystemFont,sans-serif}
main{max-width:1100px;margin:auto}h1{font-size:25px}.muted{color:#aab6c5;font-size:13px;line-height:1.6}
.card{background:#1b2532;border:1px solid #344153;border-radius:14px;padding:14px;margin:14px 0;overflow:auto}
button{background:#347eea;color:white;border:0;border-radius:9px;padding:11px 16px;font-size:15px}
table{border-collapse:collapse;width:100%;font-size:13px;min-width:760px}
td,th{padding:9px 6px;border-bottom:1px solid #354253;text-align:left}
.good{color:#5ee0a0}.warn{color:#ffbd61}.bad{color:#ff7777}
</style></head><body><main>
<h1>Investment Watchdog</h1>
<div class="muted">保有銘柄・BTC価格監視 / テクニカルシグナル / 稼働点検</div>
<div class="card"><div id="state">読み込み中…</div>
<p><button onclick="refreshData()">今すぐ更新</button></p>
<div class="muted" id="updated"></div></div>
<div class="card"><table><thead><tr>
<th>銘柄</th><th>価格</th><th>前日比</th><th>保有数</th><th>シグナル</th><th>RSI</th><th>買付目安</th>
</tr></thead><tbody id="rows"></tbody></table></div>
<div class="card"><b>自動実行スケジュール（日本時間）</b>
<p class="muted">毎時00分：保有銘柄・BTCの価格とシグナル確認<br>
毎時10分：稼働点検<br>
毎日08:30 / 16:30 / 20:30：市場レポート<br>
毎日09:00 / 21:00：簡潔な稼働報告</p></div>
<div class="card"><b>注意事項</b><p class="muted">
価格データはYahoo Finance経由で、遅延・欠損する場合があります。
シグナルはルールベースの参考情報です。証券口座への発注、ChatGPTプッシュ通知、
利益保証は行いません。通知にはTelegramまたはWebhookの設定が必要です。
</p></div>
</main><script>
async function load(url){
 try{
  const r=await fetch(url); const j=await r.json();
  if(!r.ok) throw Error(j.error||'取得失敗');
  document.getElementById('state').textContent=`稼働応答あり｜${j.items.length}銘柄｜更新 ${j.updated_at}`;
  document.getElementById('updated').textContent='価格取得元：Yahoo Finance。各銘柄のデータ日も確認してください。';
  document.getElementById('rows').innerHTML=j.items.map(x=>{
   const cls=['BUY_NOW','CHANCE'].includes(x.signal_code)?'good':
             ['WARNING','SELL_NOW','ERROR'].includes(x.signal_code)?'bad':'warn';
   const price=x.price==null?'取得不可':Number(x.price).toLocaleString('ja-JP');
   const change=x.change_pct==null?'—':`${x.change_pct}%`;
   const guide=x.order_guide&&x.order_guide.shares>0?
     `${Number(x.order_guide.budget_yen).toLocaleString('ja-JP')}円 / ${x.order_guide.shares}株`:'—';
   return `<tr><td>${x.name}<div class="muted">${x.ticker}</div></td>
   <td>${price}</td><td>${change}</td><td>${x.shares??'未登録'}</td>
   <td class="${cls}">${x.signal}<div class="muted">${x.reason||''}</div></td>
   <td>${x.rsi14??'—'}</td><td>${guide}</td></tr>`;
  }).join('');
 }catch(e){document.getElementById('state').textContent='エラー：'+e.message;}
}
async function refreshData(){await load('/api/refresh')}
load('/api/portfolio');
</script></body></html>"""


@app.get("/")
def home():
    return render_template_string(PAGE)


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "investment-watchdog", "time_jst": now_jst()}), 200


@app.get("/status")
def status():
    with connect_db() as con:
        last_runs = [
            dict(r) for r in con.execute(
                "SELECT task,status,created_at,detail FROM runs ORDER BY id DESC LIMIT 20"
            ).fetchall()
        ]
    return jsonify({
        "service": "investment-watchdog",
        "status": "running",
        "checked_at_jst": now_jst(),
        "uptime_seconds": int(time.time() - started_at),
        "scheduler_enabled": RUN_SCHEDULER,
        "scheduler_running": bool(scheduler and scheduler.running),
        "jobs": [j.id for j in scheduler.get_jobs()] if scheduler and scheduler.running else [],
        "notification_configured": bool(
            (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) or WEBHOOK_URL
        ),
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "webhook_configured": bool(WEBHOOK_URL),
        "brokerage_connected": False,
        "automatic_order_execution": False,
        "chatgpt_push_notifications": False,
        "last_runs": last_runs,
    }), 200


@app.get("/api/portfolio")
def api_portfolio():
    try:
        return jsonify(collect()), 200
    except Exception as exc:
        log.exception("Portfolio API failed")
        return jsonify({"error": str(exc)[:300]}), 502


@app.get("/api/refresh")
def api_refresh():
    try:
        return jsonify(collect(force=True)), 200
    except Exception as exc:
        log.exception("Refresh failed")
        return jsonify({"error": str(exc)[:300]}), 502


@app.get("/api/events")
def api_events():
    limit = min(max(request.args.get("limit", default=50, type=int), 1), 200)
    with connect_db() as con:
        rows = [
            dict(r) for r in con.execute(
                "SELECT id,created_at,category,title,body FROM events ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        ]
    return jsonify({"events": rows}), 200


@app.get("/api/config")
def api_config():
    return jsonify({
        "timezone": str(JST),
        "cache_seconds": CACHE_SECONDS,
        "alert_budget_yen": ALERT_BUDGET_YEN,
        "cash_available_yen": CASH_AVAILABLE_YEN,
        "portfolio_value_yen": PORTFOLIO_VALUE_YEN,
        "max_position_pct": MAX_POSITION_PCT,
        "notification_configured": bool(
            (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) or WEBHOOK_URL
        ),
        "scheduler_enabled": RUN_SCHEDULER,
        "note": "予算・現金・資産総額が未設定なら買付目安は算出しません。",
    }), 200


@app.post("/api/test-notification")
def test_notification():
    # Optional shared secret prevents a public endpoint from being abused.
    expected = os.getenv("ADMIN_API_TOKEN", "")
    supplied = request.headers.get("X-Admin-Token", "")
    if expected and supplied != expected:
        return jsonify({"error": "unauthorized"}), 401
    if not ((TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) or WEBHOOK_URL):
        return jsonify({"ok": False, "error": "通知先が未設定です"}), 400
    sent = send_notification(f"✅ Investment Watchdog テスト通知\n{now_jst()}")
    return jsonify({"ok": bool(sent), "sent_to": sent}), (200 if sent else 502)


# --------------------------- Startup ---------------------------

init_db()
start_scheduler()

if __name__ == "__main__":
    # Flask's reloader can start the module twice; disable it to avoid duplicate jobs.
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

