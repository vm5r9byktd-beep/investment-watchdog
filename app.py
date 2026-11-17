import os
import time
import json
import sqlite3
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from threading import Lock

import yfinance as yf
import requests
from flask import Flask, jsonify, render_template_string
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

JST = ZoneInfo("Asia/Tokyo")
DB_PATH = os.getenv("DB_PATH", "watchdog.db")
CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "900"))
ALERT_BUDGET_YEN = float(os.getenv("ALERT_BUDGET_YEN", "0"))
MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", "0.10"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Asia/Tokyo")

# Shares based on the latest holdings the user supplied. Confirm/update after trades.
# BTC quantity and cash available are deliberately not guessed.
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
SIGNAL_LABELS = {
    "BUY_NOW": "🔴【即時買い】",
    "CHANCE": "🟡【チャンス】",
    "WARNING": "🟠【注意】",
    "SELL_NOW": "🔵【即時売り】",
    "HOLD": "⚪【様子見】",
    "ERROR": "⚫【データ未取得】",
}
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("investment-watchdog")
started_at = time.time()
cache = {"at": 0, "payload": None}
cache_lock = Lock()
scheduler = None

PAGE = r"""<!doctype html><html lang="ja"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Investment Watchdog</title>
<style>
body{margin:0;padding:16px;background:#10151d;color:#eef2f7;font-family:-apple-system,BlinkMacSystemFont,sans-serif}
main{max-width:980px;margin:auto}h1{font-size:25px}.muted{color:#aab6c5;font-size:13px;line-height:1.6}
.card{background:#1b2532;border:1px solid #344153;border-radius:14px;padding:14px;margin:14px 0}
button{background:#347eea;color:white;border:0;border-radius:9px;padding:11px 16px;font-size:15px}
table{border-collapse:collapse;width:100%;font-size:13px}td,th{padding:9px 5px;border-bottom:1px solid #354253;text-align:left}
.good{color:#5ee0a0}.warn{color:#ffbd61}.bad{color:#ff7777}
</style></head><body><main>
<h1>Investment Watchdog</h1><div class="muted">定期監視・保有銘柄・BTC・シグナル・稼働記録</div>
<div class="card"><div id="state">読み込み中…</div><p><button onclick="refresh()">今すぐ更新</button></p><div class="muted" id="updated"></div></div>
<div class="card"><table><thead><tr><th>銘柄</th><th>価格</th><th>保有数</th><th>シグナル</th><th>RSI</th></tr></thead><tbody id="rows"></tbody></table></div>
<div class="card"><b>自動実行設定</b><p class="muted">毎時00分：市場・保有銘柄・BTCの確認<br>毎日08:30 / 16:30 / 20:30：市場・資産レポート<br>毎時：稼働点検、連続失敗を検知<br>毎日09:00 / 21:00：簡潔な稼働報告</p></div>
<div class="card muted">注意：データはYahoo Finance経由で、遅延・欠損の可能性があります。シグナルはルールベースの参考情報で、投資助言や利益保証ではありません。証券口座への発注、ChatGPT内タスクの操作、ChatGPTプッシュ通知は行いません。通知はTelegramまたはWebhookを別途設定した場合のみ送信します。</div>
</main><script>
async function load(url='/api/portfolio'){try{let r=await fetch(url);let j=await r.json();if(!r.ok)throw Error(j.error||'取得失敗');
document.getElementById('state').textContent=`稼働中｜${j.items.length}銘柄｜最新更新 ${j.updated_at}`;
document.getElementById('updated').textContent='価格取得元：Yahoo Finance。価格時点は各銘柄のデータ日を確認してください。';
document.getElementById('rows').innerHTML=j.items.map(x=>`<tr><td>${x.name}<div class="muted">${x.ticker}</div></td><td>${x.price==null?'取得不可':Number(x.price).toLocaleString('ja-JP')}</td><td>${x.shares??'未登録'}</td><td class="${x.signal_code==='WARNING'||x.signal_code==='SELL_NOW'?'bad':x.signal_code==='CHANCE'||x.signal_code==='BUY_NOW'?'good':'warn'}">${x.signal}</td><td>${x.rsi14??'—'}</td></tr>`).join('');
}catch(e){document.getElementById('state').textContent='エラー：'+e.message;}}
async function refresh(){await load('/api/refresh')} load();
</script></body></html>"""

def db():
    con = sqlite3.connect(DB_PATH, timeout=20)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with db() as con:
        con.execute("""CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
            category TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL)""")
        con.execute("""CREATE TABLE IF NOT EXISTS runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
            task TEXT NOT NULL, status TEXT NOT NULL, detail TEXT)""")

def now_jst():
    return datetime.now(JST).isoformat(timespec="seconds")

def log_run(task, status, detail=""):
    with db() as con:
        con.execute("INSERT INTO runs(created_at,task,status,detail) VALUES(?,?,?,?)",
                    (now_jst(), task, status, str(detail)[:2000]))

def record_event(category, title, body):
    with db() as con:
        con.execute("INSERT INTO events(created_at,category,title,body) VALUES(?,?,?,?)",
                    (now_jst(), category, title, body[:5000]))

def send_notification(message):
    sent = []
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
                timeout=15)
            response.raise_for_status()
            sent.append("telegram")
        except Exception:
            log.exception("Telegram notification failed")
    if WEBHOOK_URL:
        try:
            response = requests.post(WEBHOOK_URL, json={"text": message, "message": message}, timeout=15)
            response.raise_for_status()
            sent.append("webhook")
        except Exception:
            log.exception("Webhook notification failed")
    return sent

def fetch_one(item):
    try:
        hist = yf.Ticker(item["ticker"]).history(period="6mo", interval="1d", auto_adjust=True)
        if hist is None or hist.empty:
            raise ValueError("価格履歴なし")
        close = hist["Close"].dropna()
        if len(close) < 30:
            raise ValueError("分析可能な履歴が不足")
        price = float(close.iloc[-1])
        sma25 = float(close.rolling(25).mean().iloc[-1])
        sma75 = float(close.rolling(75).mean().iloc[-1]) if len(close) >= 75 else None
        delta = close.diff()
        avg_gain = delta.clip(lower=0).rolling(14).mean()
        avg_loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = avg_gain / avg_loss.replace(0, float("nan"))
        rsi_s = 100 - 100 / (1 + rs)
        rsi = float(rsi_s.iloc[-1]) if rsi_s.notna().any() else None

        code, reason = "HOLD", "明確な条件なし"
        # Rule-based indicators, not a standalone trading recommendation.
        if sma75 is not None and price > sma25 > sma75 and (rsi is None or 45 <= rsi <= 68):
            code, reason = "CHANCE", "株価が25日線・75日線を上回る上昇基調"
        if rsi is not None and rsi >= 75:
            code, reason = "WARNING", "RSIが75以上。短期過熱の可能性"
        if price < sma25 and sma75 is not None and sma25 < sma75:
            code, reason = "WARNING", "株価が25日線を下回り、移動平均も弱い"
        if rsi is not None and rsi < 25 and price > sma75 if sma75 is not None else False:
            code, reason = "CHANCE", "RSIが低水準。反発確認が必要"
        # No BUY_NOW / SELL_NOW solely from one indicator; reserve those for corroborated rules.
        return {
            **item, "price": round(price, 3), "sma25": round(sma25, 3),
            "sma75": round(sma75, 3) if sma75 is not None else None,
            "rsi14": round(rsi, 1) if rsi is not None else None,
            "signal_code": code, "signal": SIGNAL_LABELS[code],
            "reason": reason, "data_date": str(close.index[-1]), "error": None,
        }
    except Exception as exc:
        return {**item, "price": None, "sma25": None, "sma75": None, "rsi14": None,
                "signal_code": "ERROR", "signal": SIGNAL_LABELS["ERROR"],
                "reason": str(exc)[:160], "data_date": None, "error": str(exc)}

def collect(force=False):
    with cache_lock:
        if not force and cache["payload"] and time.time() - cache["at"] < 600:
            return cache["payload"]
    items = [fetch_one(x) for x in HOLDINGS]
    payload = {"service": "investment-watchdog", "status": "running",
               "updated_at": now_jst(), "items": items}
    with cache_lock:
        cache["at"], cache["payload"] = time.time(), payload
    return payload

def portfolio_monitor():
    try:
        data = collect(force=True)
        significant = [x for x in data["items"] if x["signal_code"] in ("CHANCE", "WARNING", "BUY_NOW", "SELL_NOW")]
        lines = []
        for x in significant:
            lines.append(f'{x["signal"]} {x["name"]} {x["ticker"]}: price={x["price"]}, RSI={x["rsi14"]}, {x["reason"]}')
        body = "\n".join(lines) if lines else "今回の確認では、通知対象となる明確なシグナルはありません。"
        record_event("monitor", "定期投資監視", body)
        if significant:
            send_notification("投資監視レポート\n" + body)
        log_run("hourly_portfolio_monitor", "success", f'{len(data["items"])} symbols')
    except Exception as exc:
        log.exception("Portfolio monitor failed")
        log_run("hourly_portfolio_monitor", "failed", exc)

def market_report():
    try:
        data = collect(force=True)
        summary = [f'{x["name"]}: {x["price"] if x["price"] is not None else "取得不可"} / {x["signal"]}' for x in data["items"]]
        body = "定期市場・資産レポート\n" + "\n".join(summary)
        record_event("report", "市場・資産レポート", body)
        send_notification(body)
        log_run("market_report", "success", "scheduled report")
    except Exception as exc:
        log.exception("Market report failed")
        log_run("market_report", "failed", exc)

def health_audit():
    try:
        # Local app heartbeat and last task outcomes; does not prove external provider availability.
        with db() as con:
            failures = con.execute("""SELECT COUNT(*) FROM runs
                WHERE status='failed' AND created_at >= ?""",
                (datetime.now(JST).replace(hour=0, minute=0, second=0).isoformat(),)).fetchone()[0]
        status = "success" if failures < 3 else "warning"
        detail = f"today_failed_runs={failures}; process_alive=true"
        log_run("hourly_health_audit", status, detail)
        if failures >= 3:
            msg = "🟠 稼働点検：本日3回以上のタスク失敗を記録。ログ確認が必要です。\n" + detail
            record_event("health", "稼働点検警告", msg)
            send_notification(msg)
    except Exception as exc:
        log.exception("Health audit failed")
        log_run("hourly_health_audit", "failed", exc)

def routine_status_notice():
    with db() as con:
        rows = con.execute("SELECT task,status,created_at FROM runs ORDER BY id DESC LIMIT 8").fetchall()
    body = "稼働点検（簡潔版）\n" + "\n".join(
        f'{r["task"]}: {r["status"]} ({r["created_at"]})' for r in rows)
    record_event("health", "定時稼働報告", body)
    send_notification(body)

def start_scheduler():
    global scheduler
    if scheduler is not None and scheduler.running:
        return
    scheduler = BackgroundScheduler(timezone=JST)
    scheduler.add_job(portfolio_monitor, CronTrigger(minute=0), id="hourly_portfolio", replace_existing=True, max_instances=1, coalesce=True)
    scheduler.add_job(health_audit, CronTrigger(minute=10), id="hourly_audit", replace_existing=True, max_instances=1, coalesce=True)
    for hour in (8, 16, 20):
        minute = 30 if hour in (8, 16, 20) else 0
        scheduler.add_job(market_report, CronTrigger(hour=hour, minute=minute), id=f"report_{hour}_{minute}", replace_existing=True, max_instances=1, coalesce=True)
    scheduler.add_job(routine_status_notice, CronTrigger(hour="9,21", minute=0), id="status_notice", replace_existing=True, max_instances=1, coalesce=True)
    scheduler.start()
    log.info("Scheduler started in JST")

@app.get("/")
def home():
    return render_template_string(PAGE)

@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "investment-watchdog", "time_jst": now_jst()}), 200

@app.get("/status")
def status():
    with db() as con:
        last_runs = [dict(r) for r in con.execute(
            "SELECT task,status,created_at,detail FROM runs ORDER BY id DESC LIMIT 12").fetchall()]
    return jsonify({
        "service": "investment-watchdog", "status": "running",
        "checked_at_jst": now_jst(), "uptime_seconds": int(time.time()-started_at),
        "scheduler_running": bool(scheduler and scheduler.running),
        "jobs": [j.id for j in scheduler.get_jobs()] if scheduler and scheduler.running else [],
        "notification_configured": bool((TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) or WEBHOOK_URL),
        "brokerage_connected": False, "automatic_order_execution": False,
        "chatgpt_task_integration": False, "last_runs": last_runs
    }), 200

@app.get("/api/portfolio")
def api_portfolio():
    try:
        return jsonify(collect()), 200
    except Exception as exc:
        log.exception("Portfolio API failed")
        return jsonify({"error": str(exc)[:200]}), 502

@app.get("/api/refresh")
def api_refresh():
    try:
        return jsonify(collect(force=True)), 200
    except Exception as exc:
        log.exception("Refresh failed")
        return jsonify({"error": str(exc)[:200]}), 502

@app.get("/api/events")
def api_events():
    with db() as con:
        rows = [dict(r) for r in con.execute(
            "SELECT id,created_at,category,title,body FROM events ORDER BY id DESC LIMIT 50").fetchall()]
    return jsonify({"events": rows})

@app.get("/api/config")
def api_config():
    return jsonify({
        "timezone": APP_TIMEZONE, "cache_seconds": CACHE_SECONDS,
        "alert_budget_yen": ALERT_BUDGET_YEN,
        "max_position_pct": MAX_POSITION_PCT,
        "notification_configured": bool((TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) or WEBHOOK_URL),
        "note": "予算0円の間は買付金額を算出しません。保有数・BTC数量・現金を実際の値に更新してください。"
    })

init_db()
start_scheduler()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
