# Investment Watchdog（Render用スターター）

このZIPは、GitHubの `investment-watchdog` リポジトリにアップロードし、Renderで起動するための最小構成です。

## 含まれるファイル

- `app.py`：小さなWebサービス。`/health` と `/status` を提供します
- `requirements.txt`：Python依存パッケージ
- `render.yaml`：Renderのデプロイ設定

## 重要な範囲・制限

この版が確認するのは、**Render上のサービス自体が起動して応答するか**という稼働確認です。
証券口座への接続、株価・決算・ニュースの取得、売買シグナル判定、ChatGPTの既存タスクの状態取得や通知連携はまだ実装していません。これらが連携済みであるかのようには扱いません。

## iPhoneでのセットアップ

### 1. ZIPを展開する
1. ChatGPTのZIPリンクをタップします。
2. iPhoneの「ファイル」アプリに保存します（共有メニューが出た場合は「ファイルに保存」）。
3. 「ファイル」アプリで `investment_watchdog_render_package.zip` を開きます。
4. 同じ場所に展開された `investment_watchdog_render_package` フォルダを開きます。

### 2. GitHubへ4ファイルをアップロード
1. SafariでGitHubにログインします。
2. 自分の `investment-watchdog` リポジトリを開きます（PrivateのままでOK）。
3. `Add file` → `Upload files` を選びます。
4. 展開したフォルダ内の `app.py`、`requirements.txt`、`render.yaml`、`README_JA.md` を選択してアップロードします。
5. 画面下部の `Commit changes` を押します。ブランチは通常 `main` のままで構いません。

### 3. Renderへ接続・デプロイ
1. Safariで https://render.com/ を開き、アカウントにログインします。
2. Dashboardで `New` → `Web Service` を選びます。
3. GitHubを接続し、`investment-watchdog` リポジトリへのアクセスを許可します。
4. 対象リポジトリを選択します。
5. `render.yaml` が認識されたら設定を確認し、`Apply` / `Create Web Service` で作成します。
6. Deployが完了するまで待ちます。

### 4. 起動確認
Renderのサービス画面に表示されるURLを開きます。
- URL末尾 `/health`：`{"ok":true,...}` が返れば正常応答
- URL末尾 `/status`：稼働状態と起動後の経過秒数が返ります

## トラブル時
- GitHubへのアップロードで失敗したら、4ファイルがZIPの中から展開されているか確認します（ZIP自体をリポジトリに置くのではありません）。
- RenderのBuild Logs / Deploy Logsにエラーがあれば、その画面を共有してください。エラー内容を見て次の操作を案内します。
- 無料プランなどではスリープや利用制限がある場合があります。常時監視を保証するものではありません。
