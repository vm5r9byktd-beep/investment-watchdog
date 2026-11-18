"""
Investment Watchdog — risk-controlled monitoring service
- No brokerage connection and NO automatic orders.
- Signals are screening flags, not personalized investment advice.
- Data errors/stale data always become DATA_ERROR, never BUY/SELL.
- SELL_NOW is deliberately hard to trigger and requires multiple independent
  technical confirmations plus persistence across scheduled runs.
"""
from __future__ import annotations

import os
import time
import json
import sqlite3
import logging
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from threading import Lock
from typing import Any

import requests
import yfinance as yf
import pandas as pd
from flask import Flask, jsonify, render_template_string, request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

JST = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Tokyo"))
DB_PATH = os.getenv("DB_PATH", "watchdog.db")
CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "300"))
DATA_MAX_AGE_HOURS = float(os.getenv("DATA_MAX_AGE_HOURS", "72"))
ALERT_BUDGET_YEN = float(os.getenv("ALERT_BUDGET_YEN", "0"))
MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", "0.10"))
MIN_ORDER_YEN = float(os.getenv("MIN_ORDER_YEN", "10000"))
MAX_ALERTS_PER_DAY = int(os.getenv("MAX_ALERTS_PER_DAY", "8"))
SELL_CONFIRM_RUNS = int(os.getenv("SELL_CONFIRM_RUNS", "3"))
BUY_CONFIRM_RUNS = int(os.getenv("BUY_CONFIRM_RUNS", "2"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
ALERTS_ENABLED = os.getenv("ALERTS_ENABLED", "false").lower() == "true"
ENABLE_ROUTINE_NOTICES = os.getenv("ENABLE_ROUTINE_NOTICES", "true").lower() == "true"
PORT = int(os.getenv("PORT", "10000"))

# Update this list after every trade. BTC quantity and cash are intentionally unknown.
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

LABELS = {
    "BUY_NOW": "🔴【即時買い候補】",
    "CHANCE": "🟡【チャンス】",
    "WARNING": "🟠【注意】",
    "SELL_NOW": "🔵【売却要検討】",
    "HOLD": "⚪【様子見】",
    "DATA_ERROR": "⚫【データ未取得】",
}
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("investment-watchdog")
started_at = time.time()
cache: dict[str, Any] = {"at": 0, "payload": None}
cache_lock = Lock()
scheduler: BackgroundScheduler | None = None
monitor_lock = Lock()

PAGE = r"""<!doctype html><html lang="ja"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Investment Watchdog</title><style>
*{box-sizing:border-box}body{margin:0;padding:14px;background:#10151d;color:#eef2f7;font-family:-apple-system,BlinkMacSystemFont,sans-serif}
main{max-width:1100px;margin:auto}h1{font-size:24px}.muted{color:#aab6c5;font-size:13px;line-height:1.6}
.card{background:#1b2532;border:1px solid #344153;border-radius:14px;padding:14px;margin:14px 0;overflow:auto}
button{background:#347eea;color:white;border:0;border-radius:9px;padding:11px 16px;font-size:15px}
table{border-collapse:collapse;width:100%;min-width:900px;font-size:13px}td,th{padding:10px 7px;border-bottom:1px solid #354253;text-align:left;vertical-align:top}
.signal{font-weight:700;white-space:nowrap}.reason{min-width:260px}.good{color:#5ee0a0}.warn{color:#ffbd61}.bad{color:#ff8585}
.pill{padding:3px 7px;border-radius:7px;background:#263548}.small{font-size:12px}
</style></head><body><main>
<h1>Investment Watchdog</h1><div class="muted">監視・テクニカル判定・通知・稼働点検（自動売買は行いません）</div>
<div class="card"><div id="state">読み込み中…</div><p><button onclick="refresh()">今すぐ更新</button>
<button onclick="testNotice()">通知テスト</button></p><div class="muted" id="updated"></div></div>
<div class="card"><b>保有銘柄・シグナル</b><table><thead><tr>
<th>銘柄</th><th>価格</th><th>前日比</th><th>保有数</th><th>シグナル</th><th>RSI</th><th>判断理由</th><th>買付目安</th><th>売却検討数</th>
</tr></thead><tbody id="rows"></tbody></table></div>
<div class="card"><b>安全上の注意</b><div class="muted">
即時買い候補・売却要検討は自動発注ではありません。データ不足・古いデータは必ずデータ未取得として扱います。
売買判断には決算、ニュース、保有目的、取得単価、税金、資金余力などの確認が別途必要です。
</div></div>
<div class="card"><b>自動実行スケジュール（日本時間）</b><div class="muted">
毎時00分：保有銘柄・BTC監視／毎時10分：稼働点検<br>
毎日08:30・16:30・20:30：市場レポート／毎日09:00・21:00：稼働報告
</div></div>
</main><script>
async function load(url='/api/portfolio'){try{let r=await fetch(url);let j=await r.json();if(!r.ok)throw Error(j.error||'取得失敗');
document.getElementById('state').textContent=`${j.status==='running'?'稼働応答あり':'状態確認'}｜${j.items.length}銘柄｜更新 ${j.updated_at}`;
document.getElementById('updated').textContent='データ日・鮮度・取得元を確認して判断してください。';
document.getElementById('rows').innerHTML=j.items.map(x=>`<tr>
<td>${x.name}<div class="muted">${x.ticker}</div></td>
<td>${x.price==null?'取得不可':Number(x.price).toLocaleString('ja-JP')}</td>
<td>${x.change_pct==null?'—':x.change_pct.toFixed(2)+'%'}</td>
<td>${x.shares??'未登録'}</td>
<td class="signal">${x.signal}</td><td>${x.rsi14??'—'}</td>
<td class="reason">${x.reason||''}<div class="muted small">データ日: ${x.data_date||'—'} / ${x.freshness||'—'}</div></td>
<td>${x.buy_amount_yen?x.buy_amount_yen.toLocaleString('ja-JP')+'円':'算出なし'}<div class="muted">${x.buy_shares??'—'}株</div></td>
<td>${x.sell_shares??'—'}株<div class="muted">${x.sell_amount_yen?x.sell_amount_yen.toLocaleString('ja-JP')+'円':'—'}</div></td>
</tr>`).join('');
}catch(e){document.getElementById('state').textContent='エラー：'+e.message;}}
async function refresh(){await load('/api/refresh')}
async function testNotice(){try{let r=await fetch('/api/test-notification',{method:'POST'});let j=await r.json();alert(j.message||j.error||'結果を確認してください')}catch(e){alert(e.message)}}
load();
</script></body></html>"""

def now_jst() -> datetime:
    return datetime.now(JST)

def db():
    con = sqlite3.connect(DB_PATH, timeout=20)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    with db() as con:
        con.execute("""CREATE TABLE IF NOT EXISTS runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
            task TEXT NOT NULL, status TEXT NOT NULL, detail TEXT)""")
        con.execute("""CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
            category TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL)""")
        con.execute("""CREATE TABLE IF NOT EXISTS signal_state(
            ticker TEXT PRIMARY KEY, buy_streak INTEGER NOT NULL DEFAULT 0,
            sell_streak INTEGER NOT NULL DEFAULT 0, last_code TEXT, updated_at TEXT)""")
        con.execute("""CREATE TABLE IF NOT EXISTS sent_alerts(
            fingerprint TEXT PRIMARY KEY, sent_at TEXT NOT NULL, ticker TEXT, code TEXT)""")

def log_run(task: str, status: str, detail: str = ""):
    with db() as con:
        con.execute("INSERT INTO runs(created_at,task,status,detail) VALUES(?,?,?,?)",
                    (now_jst().isoformat(timespec="seconds"), task, status, str(detail)[:2000]))

def record_event(category: str, title: str, body: str):
    with db() as con:
        con.execute("INSERT INTO events(created_at,category,title,body) VALUES(?,?,?,?)",
                    (now_jst().isoformat(timespec="seconds"), category, title, body[:5000]))

def notify(message: str) -> dict:
    if not ALERTS_ENABLED:
        return {"sent": [], "disabled": True, "detail": "ALERTS_ENABLED=false"}
    sent, errors = [], []
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                              json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=15)
            r.raise_for_status()
            if not r.json().get("ok"):
                raise RuntimeError("Telegram API returned ok=false")
            sent.append("telegram")
        except Exception as exc:
            errors.append(f"telegram:{exc}")
            log.exception("Telegram notification failed")
    if WEBHOOK_URL:
        try:
            r = requests.post(WEBHOOK_URL, json={"text": message, "message": message}, timeout=15)
            r.raise_for_status()
            sent.append("webhook")
        except Exception as exc:
            errors.append(f"webhook:{exc}")
            log.exception("Webhook notification failed")
    return {"sent": sent, "errors": errors, "disabled": False}

def calc_rsi(close: pd.Series, period: int = 14) -> float | None:
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    if loss.iloc[-1] == 0:
        return 100.0 if gain.iloc[-1] > 0 else 50.0
    value = 100 - 100 / (1 + gain.iloc[-1] / loss.iloc[-1])
    return float(value) if pd.notna(value) else None

def get_confirmation(ticker: str, buy_raw: bool, sell_raw: bool) -> tuple[int, int]:
    with db() as con:
        row = con.execute("SELECT buy_streak,sell_streak FROM signal_state WHERE ticker=?", (ticker,)).fetchone()
        buy_streak = (row["buy_streak"] if row else 0) + 1 if buy_raw else 0
        sell_streak = (row["sell_streak"] if row else 0) + 1 if sell_raw else 0
        con.execute("""INSERT INTO signal_state(ticker,buy_streak,sell_streak,updated_at)
            VALUES(?,?,?,?) ON CONFLICT(ticker) DO UPDATE SET
            buy_streak=excluded.buy_streak,sell_streak=excluded.sell_streak,updated_at=excluded.updated_at""",
            (ticker, buy_streak, sell_streak, now_jst().isoformat(timespec="seconds")))
    return buy_streak, sell_streak

def fetch_one(item: dict) -> dict:
    base = {**item, "price": None, "change_pct": None, "rsi14": None,
            "signal_code": "DATA_ERROR", "signal": LABELS["DATA_ERROR"],
            "reason": "価格データを取得できないため判定を停止", "data_date": None,
            "freshness": "不明", "buy_amount_yen": None, "buy_shares": None,
            "sell_shares": None, "sell_amount_yen": None, "error": None}
    try:
        hist = yf.Ticker(item["ticker"]).history(period="1y", interval="1d", auto_adjust=True)
        if hist is None or hist.empty or "Close" not in hist:
            raise ValueError("価格履歴なし")
        close = hist["Close"].dropna()
        if len(close) < 80:
            raise ValueError("分析可能な価格履歴が80営業日未満")
        last_index = close.index[-1]
        data_dt = last_index.to_pydatetime()
        if data_dt.tzinfo is None:
            data_dt = data_dt.replace(tzinfo=JST)
        age_hours = max(0.0, (now_jst() - data_dt.astimezone(JST)).total_seconds()/3600)
        # yfinance daily bar timestamp may represent midnight; freshness is conservative.
        fresh = age_hours <= DATA_MAX_AGE_HOURS
        if not fresh:
            raise ValueError(f"価格データが古い可能性（{age_hours:.1f}時間）")
        price = float(close.iloc[-1])
        prev = float(close.iloc[-2]) if len(close) >= 2 else None
        change = ((price / prev)-1)*100 if prev else None
        sma20 = float(close.rolling(20).mean().iloc[-1])
        sma50 = float(close.rolling(50).mean().iloc[-1])
        sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
        rsi = calc_rsi(close)
        ret20 = float(close.pct_change(20).iloc[-1] * 100)
        ret60 = float(close.pct_change(60).iloc[-1] * 100)
        vol20 = float(close.pct_change().rolling(20).std().iloc[-1] * (252**0.5) * 100)
        # A single indicator cannot trigger a trade flag.
        trend_bear = price < sma20 < sma50
        long_bear = sma200 is not None and price < sma200 and sma50 < sma200
        momentum_bear = ret20 < -8 and ret60 < -12
        rsi_weak = rsi is not None and rsi < 35
        sell_raw = bool(trend_bear and long_bear and momentum_bear and rsi_weak)
        trend_bull = price > sma20 > sma50 and (sma200 is None or sma50 > sma200)
        momentum_bull = ret20 > 5 and ret60 > 8
        rsi_ok = rsi is not None and 50 <= rsi <= 68
        buy_raw = bool(trend_bull and momentum_bull and rsi_ok)
        buy_streak, sell_streak = get_confirmation(item["ticker"], buy_raw, sell_raw)
        if sell_streak >= SELL_CONFIRM_RUNS:
            code, reason = "SELL_NOW", "複数条件の弱気トレンドが複数回継続。売却を要検討（自動売却なし）"
        elif sell_raw:
            code, reason = "WARNING", f"弱気条件を検出。継続確認中 {sell_streak}/{SELL_CONFIRM_RUNS}回"
        elif buy_streak >= BUY_CONFIRM_RUNS:
            code, reason = "BUY_NOW", "複数条件の上昇トレンドが複数回継続。買付候補（自動発注なし）"
        elif buy_raw:
            code, reason = "CHANCE", f"強気条件を検出。継続確認中 {buy_streak}/{BUY_CONFIRM_RUNS}回"
        elif rsi is not None and rsi >= 75:
            code, reason = "WARNING", "RSIが75以上。短期過熱の可能性"
        elif trend_bear or (rsi is not None and rsi < 35):
            code, reason = "WARNING", "下落傾向またはRSI低水準。売却確定ではなく要確認"
        elif trend_bull:
            code, reason = "CHANCE", "上昇トレンド候補。押し目・出来高・決算を追加確認"
        else:
            code, reason = "HOLD", "複数条件で明確な売買シグナルなし"
        buy_amount = None
        buy_shares = None
        if code in ("BUY_NOW", "CHANCE") and ALERT_BUDGET_YEN >= MIN_ORDER_YEN and item["kind"] == "stock":
            budget = min(ALERT_BUDGET_YEN, max(0, ALERT_BUDGET_YEN * MAX_POSITION_PCT / max(MAX_POSITION_PCT, 0.01)))
            buy_shares = int(budget // price)
            buy_amount = int(buy_shares * price) if buy_shares > 0 else None
        sell_shares = None
        sell_amount = None
        if code == "SELL_NOW" and item["kind"] == "stock" and item.get("shares"):
            sell_shares = int(item["shares"])
            sell_amount = int(sell_shares * price)
        base.update(price=round(price, 3), change_pct=round(change, 2) if change is not None else None,
                    rsi14=round(rsi, 1) if rsi is not None else None,
                    signal_code=code, signal=LABELS[code], reason=reason,
                    data_date=str(last_index), freshness="fresh",
                    buy_amount_yen=buy_amount, buy_shares=buy_shares,
                    sell_shares=sell_shares, sell_amount_yen=sell_amount,
                    sma20=round(sma20, 3), sma50=round(sma50, 3),
                    sma200=round(sma200, 3) if sma200 else None,
                    return20_pct=round(ret20, 2), return60_pct=round(ret60, 2),
                    annualized_volatility_pct=round(vol20, 2))
        return base
    except Exception as exc:
        base["error"] = str(exc)[:300]
        base["reason"] = f"データ不足・取得失敗のため売買判定を停止: {str(exc)[:180]}"
        # Fail-safe: never let an error inherit a previous sell/buy label.
        return base

def collect(force: bool = False) -> dict:
    with cache_lock:
        if not force and cache["payload"] and time.time()-cache["at"] < CACHE_SECONDS:
            return cache["payload"]
    items = [fetch_one(x) for x in HOLDINGS]
    payload = {"service": "investment-watchdog", "status": "running",
               "updated_at": now_jst().isoformat(timespec="seconds"), "items": items}
    with cache_lock:
        cache["at"], cache["payload"] = time.time(), payload
    return payload

def fingerprint(item: dict) -> str:
    raw = f'{item["ticker"]}|{item["signal_code"]}|{item.get("reason","")}'
    return hashlib.sha256(raw.encode()).hexdigest()

def alert_allowed(item: dict) -> bool:
    fp = fingerprint(item)
    today = now_jst().date().isoformat()
    with db() as con:
        count = con.execute("SELECT COUNT(*) FROM sent_alerts WHERE sent_at LIKE ?", (today+"%",)).fetchone()[0]
        exists = con.execute("SELECT 1 FROM sent_alerts WHERE fingerprint=?", (fp,)).fetchone()
        if exists or count >= MAX_ALERTS_PER_DAY:
            return False
        con.execute("INSERT INTO sent_alerts(fingerprint,sent_at,ticker,code) VALUES(?,?,?,?)",
                    (fp, now_jst().isoformat(timespec="seconds"), item["ticker"], item["signal_code"]))
    return True

def portfolio_monitor():
    if not monitor_lock.acquire(blocking=False):
        return
    try:
        data = collect(force=True)
        important = [x for x in data["items"] if x["signal_code"] in ("BUY_NOW","CHANCE","WARNING","SELL_NOW")]
        for x in important:
            msg = (f'{x["signal"]} {x["name"]} ({x["ticker"]})\n価格: {x["price"]}\n'
                   f'RSI: {x["rsi14"]}\n理由: {x["reason"]}\n'
                   f'買付目安: {x["buy_amount_yen"] or "算出なし"}円 / {x["buy_shares"] or "—"}株\n'
                   f'売却検討: {x["sell_shares"] or "—"}株（自動売却なし）')
            record_event("signal", x["name"], msg)
            if alert_allowed(x):
                notify(msg)
        log_run("hourly_portfolio_monitor", "success", f'{len(data["items"])} symbols')
    except Exception as exc:
        log.exception("Portfolio monitor failed")
        log_run("hourly_portfolio_monitor", "failed", str(exc))
    finally:
        monitor_lock.release()

def market_report():
    try:
        data = collect(force=True)
        lines = [f'{x["name"]}: {x["price"] if x["price"] is not None else "取得不可"} / {x["signal"]}'
                 for x in data["items"]]
        body = "定期市場レポート\n" + "\n".join(lines)
        record_event("report", "市場・資産レポート", body)
        notify(body)
        log_run("market_report", "success", "scheduled report")
    except Exception as exc:
        log.exception("Market report failed")
        log_run("market_report", "failed", str(exc))

def health_audit():
    try:
        with db() as con:
            failures = con.execute("""SELECT COUNT(*) FROM runs WHERE status='failed'
                AND created_at >= ?""", (now_jst().replace(hour=0,minute=0,second=0,microsecond=0).isoformat(),)).fetchone()[0]
        status = "success" if failures < 3 else "warning"
        detail = f"today_failed_runs={failures}; process_alive=true"
        log_run("hourly_health_audit", status, detail)
        if failures >= 3:
            msg = "🟠 稼働点検：本日3回以上のタスク失敗。ログ確認が必要です。\n" + detail
            record_event("health", "稼働点検警告", msg)
            notify(msg)
    except Exception as exc:
        log.exception("Health audit failed")
        log_run("hourly_health_audit", "failed", str(exc))

def routine_status_notice():
    if not ENABLE_ROUTINE_NOTICES:
        return
    try:
        with db() as con:
            rows = con.execute("SELECT task,status,created_at FROM runs ORDER BY id DESC LIMIT 8").fetchall()
        body = "稼働点検（簡潔版）\n" + "\n".join(
            f'{r["task"]}: {r["status"]} ({r["created_at"]})' for r in rows)
        record_event("health", "定時稼働報告", body)
        notify(body)
    except Exception as exc:
        log.exception("Routine notice failed")

def start_scheduler():
    global scheduler
    if scheduler is not None and scheduler.running:
        return
    scheduler = BackgroundScheduler(timezone=JST)
    scheduler.add_job(portfolio_monitor, CronTrigger(minute=0), id="hourly_portfolio",
                      replace_existing=True, max_instances=1, coalesce=True)
    scheduler.add_job(health_audit, CronTrigger(minute=10), id="hourly_audit",
                      replace_existing=True, max_instances=1, coalesce=True)
    for hour in (8,16,20):
        scheduler.add_job(market_report, CronTrigger(hour=hour,minute=30),
                          id=f"report_{hour}", replace_existing=True, max_instances=1, coalesce=True)
    scheduler.add_job(routine_status_notice, CronTrigger(hour="9,21",minute=0),
                      id="status_notice", replace_existing=True, max_instances=1, coalesce=True)
    scheduler.start()

@app.get("/")
def home():
    return render_template_string(PAGE)

@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "investment-watchdog",
                    "time_jst": now_jst().isoformat(timespec="seconds")}), 200

@app.get("/status")
def status():
    with db() as con:
        rows = [dict(r) for r in con.execute(
            "SELECT task,status,created_at,detail FROM runs ORDER BY id DESC LIMIT 12").fetchall()]
    return jsonify({"service":"investment-watchdog","status":"running",
        "uptime_seconds":int(time.time()-started_at),
        "scheduler_running":bool(scheduler and scheduler.running),
        "jobs":[j.id for j in scheduler.get_jobs()] if scheduler and scheduler.running else [],
        "notification_configured":bool((TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) or WEBHOOK_URL),
        "alerts_enabled":ALERTS_ENABLED,"brokerage_connected":False,
        "automatic_order_execution":False,"last_runs":rows}), 200

@app.get("/api/portfolio")
def api_portfolio():
    try: return jsonify(collect()), 200
    except Exception as exc:
        log.exception("Portfolio API failed")
        return jsonify({"error":str(exc)[:200]}), 502

@app.get("/api/refresh")
def api_refresh():
    try: return jsonify(collect(force=True)), 200
    except Exception as exc:
        log.exception("Refresh failed")
        return jsonify({"error":str(exc)[:200]}), 502

@app.get("/api/events")
def api_events():
    with db() as con:
        rows=[dict(r) for r in con.execute(
            "SELECT id,created_at,category,title,body FROM events ORDER BY id DESC LIMIT 50").fetchall()]
    return jsonify({"events":rows})

@app.post("/api/test-notification")
def test_notification():
    if not ALERTS_ENABLED:
        return jsonify({"ok":False,"message":"通知は安全のため無効です。環境変数 ALERTS_ENABLED=true と通知先設定後に再実行してください。"}), 409
    result=notify("Investment Watchdog 通知テスト\nこの通知が届けば通知経路は接続されています。自動売買は行いません。")
    ok=bool(result.get("sent"))
    record_event("test","通知テスト",json.dumps(result,ensure_ascii=False))
    return jsonify({"ok":ok,"message":"通知送信を要求しました。iPhone側の受信を確認してください。" if ok else "送信失敗。Renderログと通知先設定を確認してください。","result":result}), (200 if ok else 502)

@app.get("/api/config")
def api_config():
    return jsonify({"timezone":str(JST),"cache_seconds":CACHE_SECONDS,
        "data_max_age_hours":DATA_MAX_AGE_HOURS,"alert_budget_yen":ALERT_BUDGET_YEN,
        "max_position_pct":MAX_POSITION_PCT,"alerts_enabled":ALERTS_ENABLED,
        "notification_configured":bool((TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) or WEBHOOK_URL),
        "sell_confirmation_runs":SELL_CONFIRM_RUNS,
        "note":"予算0円なら買付金額は算出しません。保有株数は取引後に更新してください。"}),200

init_db()
# Render should run exactly one web process/instance when using in-process APScheduler.
start_scheduler()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
