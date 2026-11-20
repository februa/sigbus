# v2.0への移行

SigBus 2.0.0へ更新します。Python 3.13以上が必要です。
WorkerとModuleをSigBus本体に統合し、配布パッケージはsigbusひとつにします。
旧sigbus-workerの追加インストールやPYTHONPATH設定は不要です。

## StreamSpecと保存形式

すべてのStreamSpecに正の有限値の`rate_hz`（Frame/秒）を指定します。
例：`StreamSpec("signal", (1024,), "f4", rate_hz=100)`。
StateSpecにはrate_hzを指定しません。同じStreamへの接続ではrate_hzも一致させます。

File/SharedMemoryの保存形式はversion 8です。旧形式の自動移行はありません。
既存アプリケーションを停止し、新しいdirectory/namespaceを使って起動してください。
旧データの削除は不要になったことを確認してから行ってください。

## WorkerとModule

Moduleは`initialize()`、`run_once()`、`finalize()`を実装します。
SigBus用の基底クラスは`sigbus.module.Module`です。Workerが停止Eventを接続します。
`run_once()`は`RunResult.PROCESSED`、`IDLE`、`FINISHED`のいずれかを返します。

StageにModuleSpecの一覧とスケジュールを渡し、WorkerにStageの一覧を渡します。
Stage内は登録順に実行し、Stage間は別スレッドで並行実行します。
以前の`Event(condition, interval)`は`Polling(condition, interval)`へ変更してください。
`Event()`は`worker.wake(stage_name)`による通知待ちになりました。
SigBusの公開が自動で通知を発生させることはありません。

実行制御は`from sigbus.worker import ModuleSpec, RunResult, Stage, Worker`、
処理基底クラスは`from sigbus.module import Module`を使用します。
ModuleはBusと名前からSpecへのmappingを受け取り、Nodeとendpointを管理します。
共有Transportのcloseは、すべてのStage終了後にアプリケーションの所有者が行います。

## Node、手動取得、終了処理

単独endpointの既存`with acquire()`も利用できます。
Nodeでは登録endpointの資源所有権をNodeへ移管します。
複数周期で領域を保持する場合は`reserve()`→`checkout()`、
出力の確定は`commit()`、入力の返却は`complete()`、取消は`abort()`を使用します。
借用ビューをcomplete/abort後に利用しないでください。

Nodeと手動APIのcommitは非同期送信への引き渡しです。公開完了ではありません。
有限生成の完了通知前には`drain()`がTrueを返すことを確認します。
確定済みバッチの端数も公開します。停止・期限切れではFalseを返し、未送信分を保持します。
`close()`がFalseを返した場合も資源は保持されます。再試行または明示的なdiscardが必要です。
Moduleは正常終了時にdrain/close、停止時にdiscardを行います。

NodeのFrame取得はStream間のrate_hz一致、Batch取得はbatch_size/rate_hz一致を要求します。
Stateはこの比較から除外します。Batch書き込みには全Frameのtimestampsを明示します。
この検証は時刻同期や欠落補充を行うものではありません。

具体例は[Worker README](worker.md)と
[ビーム形成の実行例](../examples/beamforming/README.md)を参照してください。

## 削除した補助・互換API

- UpdateTracker：世代の比較が必要ならAPで保持・比較します。
- StreamReady：PollingにはSubscriber.has_frameを直接渡します。
- Periodic.every(seconds)：Periodic(rate_hz=1 / seconds)を使います。
- Bus.node()：Node(...)を直接生成します。
- ReadCompletePolicy：同期配信はSyncPolicyを使います。Syncの途中参加者は次の公開から参加します。
- close(drain=False)：discard()を使います。close()は通常終了、discard()は破棄終了です。
- InProcessAdapter / FileAdapter / SharedMemoryAdapter：各Transport名へ統一します。

Node.flush()はPublisherと共通の端数公開操作として残します。drainでは猶予期間も指定できます。
