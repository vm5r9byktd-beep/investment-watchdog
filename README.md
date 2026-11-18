# Investment Watchdog v2 — 4ファイル構成

## これは何か
日本株ウォッチリスト・保有資産を対象に、日次価格履歴、テクニカル指標、ニュース見出し候補、Telegram通知、保有数量同期、稼働状態をまとめる分析補助システムです。**自動発注・注文API呼出し・楽天証券へのID/パスワードログインは実装していません。**

## ファイル
- `app.py` — Flaskアプリ、SQLite、分析、通知、RSS同期API、簡易バックテスト
- `requirements.txt` — Python依存関係
- `render.yaml` — Render Blueprint
- `README.md` — セットアップと運用説明

既存GitHubリポジトリの `README_JA.md` 等は削除しないでください。このZIPの4ファイルだけを追加/置換します。

## Renderデプロイ
1. ZIPを展開し、4ファイルをGitHubリポジトリのルートへ配置してcommit/push。
2. Renderで既存サービスを変更する場合、先に現行EnvironmentとDiskを確認し、既存DBを消さないでください。Blueprint適用はサービス設定を変更することがあります。
3. RenderでBlueprintを作成/同期し、Environmentに次を入力:
   - `DASHBOARD_USER` 任意のログイン名
   - `DASHBOARD_PASSWORD` 長くランダムなパスワード
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
4. `SYNC_API_TOKEN` と `FLASK_SECRET_KEY` はRenderが自動生成します。
5. `/api/health` が `status: ok` を返すことを確認。ダッシュボード `/` はBasic認証が必要です。
6. ダッシュボードでTelegram接続テストを実行。

### Render構成上の重要点
このBlueprintはSQLite永続ディスクを使い、Webサービス1インスタンス・Gunicorn 1 workerでスケジューラを動かします。Renderの永続ディスクは単一サービスインスタンスにのみ接続可能で、複数インスタンスへスケールできません。複数worker/複数インスタンスに変更しないでください。高可用性・水平スケールが必要になった場合はPostgreSQLと独立Worker/分散ロック構成へ移行してください。
Render公式: https://render.com/docs/disks

## 楽天証券との同期 — RSSブリッジ
楽天証券の公式マーケットスピード II RSSはExcelアドインです。`RssPositionList` は保有銘柄一覧を出力し、項目には銘柄コード、銘柄名称、口座区分、保有数量、平均取得価額などがあります。RSS自体はWindows上のMicrosoft Excelとマーケットスピード IIを必要とします。
公式:
- RSS概要 https://marketspeed.jp/ms2_rss/onlinehelp/ohm_001/ohm_001_01.html
- 保有一覧・関数仕様 https://marketspeed.jp/ms2_rss/onlinehelp/ohm_002/ohm_002_05.html
- 取得項目 https://marketspeed.jp/ms2_rss/onlinehelp/ohm_002/ohm_002_07.html
- 推奨環境/準備 https://marketspeed.jp/ms2_rss/support/

### API仕様
`POST https://<RenderサービスURL>/sync/rakuten/rss`
Headers: `Authorization: Bearer <SYNC_API_TOKEN>` と `Content-Type: application/json`

Body:
```json
{
  "snapshot_at": "2026-11-18T16:00:00+09:00",
  "positions": [
    {"symbol":"7203","name":"トヨタ自動車","quantity":100,
     "average_cost":2500,"account_type":"NISA（NISA成長投資枠）"}
  ]
}
```
4桁/5桁コードは自動で `.T` を付与します。quantity必須。平均取得単価や口座区分は任意。空/不正なデータは既存保有を消しません。同一スナップショットの重複送信を抑止し、過去時刻のスナップショットによる巻き戻しを拒否します。

### Excel側の連携
このZIPは指定された4ファイルのみで構成しています。Excel VBAブリッジはアプリ本体に埋め込んでいません。以下の手順でExcelからPOSTするVBAを別途設定します。`RssPositionList` の出力列は公式ヘルプで示される項目順/Excel上のヘッダーを確認し、利用環境のExcelシートに合わせて列番号を確定してください。列の位置を推測して同期しないでください。

推奨Excelシート例:
- A1: `=RssPositionList()`
- 2行目にヘッダー、3行目以降に保有明細が展開される（RSS公式ヘルプの説明を確認）
- `symbol`, `name`, `quantity`, `average_cost`, `account_type` に対応する列を読み取る
- VBAから上記APIへJSON POST。トークンはExcelのセルや共有ブックに平文保存せず、Windows資格情報マネージャー等で管理する
- 初回は1銘柄のみテストし、同期後に画面の数量・平均取得単価・口座区分を照合してから全件同期

VBA送信例（列対応を確認した後に使う骨格。列番号は必ず実シートに合わせて修正）:
```vb
' Windows Excel + MarketSpeed II RSS が稼働しているPC上で実行
' 参照設定不要（late binding）。列番号はサンプル値なので実データで修正すること。
Sub SendHoldingsSnapshot()
    Dim ws As Worksheet, lastRow As Long, r As Long
    Dim http As Object, payload As String, token As String, baseUrl As String
    Set ws = ThisWorkbook.Worksheets("保有一覧")
    lastRow = ws.Cells(ws.Rows.Count, 1).End(xlUp).Row
    token = Environ$("WATCHDOG_SYNC_TOKEN")
    baseUrl = Environ$("WATCHDOG_URL")
    If token = "" Or baseUrl = "" Then Err.Raise 5, , "環境変数 WATCHDOG_SYNC_TOKEN / WATCHDOG_URL を設定してください"
    payload = "{""snapshot_at"":""" & Format$(Now, "yyyy-mm-dd\Thh:nn:ss") & "+09:00"",""positions"":["
    For r = 3 To lastRow
        If r > 3 Then payload = payload & ","
        payload = payload & "{""symbol"":""" & CStr(ws.Cells(r, 1).Value) & """," & _
          """name"":""" & Replace(CStr(ws.Cells(r, 2).Value), """", "\""") & """," & _
          """quantity"":" & Replace(CStr(ws.Cells(r, 4).Value), ",", "") & "," & _
          """average_cost"":" & Replace(CStr(ws.Cells(r, 6).Value), ",", "") & "," & _
          """account_type"":""" & CStr(ws.Cells(r, 3).Value) & """}"
    Next r
    payload = payload & "]}"
    Set http = CreateObject("WinHttp.WinHttpRequest.5.1")
    http.Open "POST", baseUrl & "/sync/rakuten/rss", False
    http.SetRequestHeader "Authorization", "Bearer " & token
    http.SetRequestHeader "Content-Type", "application/json"
    http.Send payload
    If http.Status < 200 Or http.Status >= 300 Then Err.Raise 5, , "同期失敗: " & http.Status & " " & http.ResponseText
    MsgBox "同期結果: " & http.ResponseText, vbInformation
End Sub
```
このVBAは列番号の例示を含む骨格であり、列対応を確認するまでは実行しないでください。日時文字列はJSTを想定。トークンをGitHubへコミットしないでください。楽天ログイン情報・取引暗証番号・OTPをWatchdogへ送らないでください。楽天RSSが動くPCを起動し、Excel/MarketSpeed IIが稼働している間だけ同期できます。Render単体からRSSを直接呼び出すものではありません。

## 分析ロジック
- SMA20/50/200、EMA20、RSI14、20/60/120営業日リターン、20日年率ボラティリティ、出来高比を表示。
- 買い候補/売却再検討は複数条件の同時成立で限定し、異なる市場日で2回確認後に通知。様子見・古いデータは通知しません。
- 「売却再検討」は必ず人が一次情報・保有理由・税/口座状況を確認するための警戒で、自動売却指示ではありません。
- Google News RSSは見出し候補の収集に限定。ニュースだけでシグナルを出しません。決算・TDnet/EDINETの一次資料を自動解析する機能は含みません。
- 株価はYahoo Finance/yfinanceの日足データ。リアルタイム価格ではなく、欠損・遅延・調整値差異があり得ます。

## 通知と株数の扱い
通知の株数/金額は、同期した保有数量とユーザー設定予算からの単純な整数株試算です。現金余力、他銘柄との集中度、売買単位、NISA残枠、税、信用取引、注文価格を完全に把握しません。したがって発注推奨ではありません。設定予算が少額でも単元株制約を自動で断定しません。

## API
- `GET /api/health` health check (public)
- `POST /sync/rakuten/rss` RSS holdings sync (Bearer token)
- `POST /sync/positions` generic holdings sync (Bearer token)
- `POST /import/csv` CSV holdings import (Bearer token)
- `GET /api/holdings` holdings (Bearer token)
- `GET /api/analysis` latest analysis (Bearer token)
- `GET /api/news` news headlines (Bearer token)
- `GET /api/alerts` alerts (Bearer token)
- `GET /api/backtest/<symbol>` simple SMA20/50 reference backtest (Bearer token)

## 重要な制約
- 本版は最高水準を目指した改善版ですが、第三者によるセキュリティ監査・Render本番環境テスト・楽天Excel環境での実機試験は未実施です。
- バックテストは簡易参考値。手数料/スリッページ/税/配当/上場廃止/ウォークフォワード等を完全再現せず、将来成績を保証しません。
- yfinanceの企業ファンダメンタルズ/決算データの網羅的な取得やTDnet/EDINET自動解析は含みません。
- Telegram API token、SYNC_API_TOKEN、DASHBOARD_PASSWORDをGitHubやチャットへ貼らないでください。
- RenderのBlueprint適用前に、既存サービス設定とDBバックアップを確認してください。
