# Investment Watchdog — 安全強化版

## 重要
- 自動売買・証券口座接続はありません。
- シグナルは投資判断の補助であり、利益や精度を保証しません。
- `SELL_NOW` は複数のテクニカル条件が複数回継続した場合だけ `売却要検討` として表示します。
- データ取得失敗・古いデータ・履歴不足は `データ未取得` とし、売買シグナルにしません。
- 決算・ニュース・事業内容の定性評価は、このコードだけでは十分に網羅できません。必ず別途確認してください。

## Render
1. GitHubリポジトリの `app.py` と `requirements.txt` を更新。
2. RenderのBuild Command: `pip install -r requirements.txt`
3. Start Command: `gunicorn app:app --workers 1 --threads 4 --timeout 120`
4. Health Check Path: `/health`
5. 無料インスタンス等のスリープ・再起動や永続ディスク未設定では、常時稼働・SQLite履歴保持を保証できません。
6. APSchedulerをWebプロセス内で動かすため、Renderは1インスタンス・1workerで運用してください。複数インスタンスに増やす場合はスケジューラーを別サービスに分離してください。

## 通知
最初は `ALERTS_ENABLED=false` のままデプロイし、画面・`/status`・`/api/portfolio` を確認してください。
TelegramのBot TokenとChat IDはRenderのEnvironment Variablesだけに登録し、チャットやGitHubに貼らないでください。
設定後に `ALERTS_ENABLED=true` として再デプロイし、`POST /api/test-notification` で通知テストします。
Telegram BotはユーザーがBotとのチャットを開始している必要があります。

## 保有数
`app.py` の `HOLDINGS` を実際の保有数に更新してください。取引後は必ず更新してください。
BTC数量は未確認のため未登録です。

## 判定の考え方
- 買い：中期トレンド・モメンタム・RSIの複数条件＋複数回確認
- 売り：短中長期トレンド・20/60営業日リターン・RSIの複数条件＋3回確認
- 1時間ごとの同一銘柄・同一シグナル再通知はSQLiteで抑制
- RSI単独やデータエラーでは即時売買判定をしません。

## 残る制約
yfinanceは遅延・欠損・仕様変更があり得ます。日本株の株価時刻、休場日、BTCの連続取引の扱いには注意してください。無料の単一プロセスSQLite構成は高可用性ではありません。
