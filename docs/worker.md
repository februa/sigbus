# WorkerとModule

SigBus内で処理単位と実行制御の責務を分けます。

- `sigbus.module.Module`：SigBusの接続・Node・endpointの所有と処理ライフサイクル。
- `sigbus.worker.Stage`：Moduleを登録順に実行するスケジュール単位。
- `sigbus.worker.Worker`：Stageごとにスレッドを起動する実行制御。

Stage間は並行実行、Stage内は逐次実行です。
WorkerはModuleの通信・資源管理を実装せず、ライフサイクルと停止通知の契約だけを使います。
ModuleはWorkerを保持・importせず、単体で初期化・処理・終了できます。
プロセス生成・Control Plane・パラメータ配送は提供しません。

配布は`sigbus`ひとつです。Python 3.13以上が必要です。
リポジトリ直下でインストール・検証します。

```sh
python -m pip install -e '.[test]'
python -m pytest -q
```

## 配置と実行例

```python
from sigbus.worker import ModuleSpec, Periodic, RunResult, Stage, Worker

class Counter:
    def initialize(self):
        self.count = 0

    def run_once(self):
        self.count += 1
        print(self.count)
        return RunResult.FINISHED if self.count == 3 else RunResult.PROCESSED

    def finalize(self):
        pass

worker = Worker([
    Stage("ab", [ModuleSpec("a", Counter()), ModuleSpec("b", Counter())],
          Periodic(rate_hz=1)),
    Stage("c", [ModuleSpec("c", Counter())], Periodic(rate_hz=1)),
])
reports = worker.run()
```

abのスレッドでa→bを順に呼び、cは別スレッドで実行します。
1 Hzのabではaとbの合計処理時間が約1秒以内であることが必要です。
別Stageは独立した実行予定を持つため、別データの処理を重ねられます。
実際の計算並列性はCPU資源、GIL、処理がGILを解放するかにも依存し、レートを保証するものではありません。

実行順序だけではデータの同一性や接続を保証しません。
取得・処理・公開・リース返却はModuleが担当し、Workerはデータ転送を中継しません。
ModuleはWorkerへの参照や継承を必要とせず、3つのメソッドを直接呼んで単体検証できます。

## Stage内の1回分の処理

Stageの条件が成立したら、登録順に各Moduleのrun_once()を最大1回呼びます。
同じStageの次回処理を重複実行しません。

| 戻り値 | Stageの動作 |
|---|---|
| PROCESSED | 次のModuleへ進む |
| IDLE | その回の残りをスキップ。次の起動は先頭Moduleから再開 |
| FINISHED | 終了予定とし、残りのModuleを今回だけ呼んでStageを終了 |

FINISHED後の後続ModuleがIDLEなら、そこで残りをスキップしてStageを終了します。
他Stageは自身の終了条件またはWorker.stop()まで継続します。
FINISHEDは下流への終了通知・データ排出・処理成功を保証しません。
Stageをまたぐ終了通知や、複数回にわたる残データの処理はアプリケーションが設計してください。

IDLE時にも前のModuleの取得・公開を取り消しません。
厳密な1件単位の連動が必要なら、入力の識別や保持をModule側で行います。
run_onceは有限な処理とし、無期限の取得待ちをしないでください。

## Periodic / Event / Polling

スケジュールはModuleごとではなくStageごとに指定します。

- `Periodic(rate_hz)`：全Stageの初期化完了後の共通基準時刻から実行予定を計算します。
  初回は即時実行可能。Stage全体の処理終了時に過ぎた予定を飛ばします。
  処理時間を周期へ加算せず、単調時計を使います。
- `Polling(condition, interval=0.001)`：初期化後に`condition(stage)`を呼び、
  引数なしの判定関数を作ります。判定は待機・データ消費をせず、実行可能ならTrueを返します。
  確認後とStage実行後のいずれもinterval秒以上空けて確認します。

Pollingは条件が継続して成立していれば再び実行します。

`Event()`は`worker.wake(stage_name)`の通知まで無期限に待ち、定期チェックしません。
通知1回でStageを1パス実行し、再び待ちます。複数通知はまとめられます。
初期化前の通知も保持し、処理中の通知は次の実行を要求します。停止は通知待ちを解除します。
IDLEでも通知は消費するため、再試行が必要なら新しい通知が必要です。
SigBusの公開が自動でwakeを呼ぶわけではありません。

秒で周期を指定する場合は`Periodic(rate_hz=1 / seconds)`を使います。
実行条件の決定はアプリケーションの責務です。

## SigBusとの接続

```python
from sigbus.worker import Polling, ModuleSpec, RunResult, Stage, Worker
from sigbus import Bus, FileTransport, NoFrameAvailable, StreamSpec

class Analysis:
    def initialize(self):
        self.transport = FileTransport(directory="/tmp/worker-demo")
        self.subscriber = Bus(self.transport).subscriber(
            StreamSpec("input", (1024,), "f4", rate_hz=100))

    def run_once(self):
        try:
            with self.subscriber.acquire() as frame:
                print(frame.payload.mean())
        except NoFrameAvailable:
            return RunResult.IDLE
        return RunResult.PROCESSED

    def finalize(self):
        if hasattr(self, "subscriber"):
            self.subscriber.close()
        if hasattr(self, "transport"):
            self.transport.close()

worker = Worker([
    Stage(name, [ModuleSpec("analysis", Analysis())],
          Polling(lambda stage: stage.module("analysis").subscriber.has_frame))
    for name in ("reader-a", "reader-b")
])
# 別Publisherが同じStreamへ公開する想定。Ctrl+Cで終了。
worker.run()
```

各Moduleが専用Subscriberを持ちます。InProcessTransportを使う場合は、同じTransport
インスタンスをアプリケーション側で共有し、全Stage終了後に所有者がcloseします。
共有Transportを個々のModuleがcloseしないでください。
各Moduleのinitialize/run_once/finalizeはすべて、そのStageの同じスレッドで実行されます。
Stage間で共有する独自の可変状態はアプリケーション側で同期してください。

判定関数はSubscriberの`has_frame`を直接渡します。確認は予約ではないため、
取得できない場合はModuleが扱います。Stateの更新検出には使用できません。

## 起動・停止・異常時

Worker.run()は全Stageの終了まで同期的に待ちます。Worker自身はプロセスを生成しません。
Moduleの構築は呼び出し側で行い、接続・資源の初期化はinitializeに置きます。
将来プロセスを分ける場合は、そのプロセス内でModuleとWorkerを構築できます。

各Stageは登録順にModuleを初期化します。全Stageの初期化完了後に処理を開始します。
initialize内で最初の入力データを待たないでください。
終了時は全Stageが実行ループを抜けた後、各Stage内で逆順にfinalizeします。
Stage間の初期化・終了順は保証しません。部分初期化でもfinalizeを呼びます。

別スレッドからのworker.stop()は全Stageの待機を起こします。
実行中のModuleが戻った後、後続Moduleの呼び出しを止めて終了します。
無期限に停止したModuleを強制終了する機能や停止期限はありません。
スレッドは非daemonであり、停止要求を出してもアプリケーション処理が戻らなければrunは戻りません。
initialize/finalize/実行条件も有限時間で戻る必要があります。

いずれかのStageの例外でWorker全体に停止を要求し、全スレッドをjoinします。
終了処理も継続して実行し、tracebackをreportsへ保存します。
通常例外はWorkerErrorの原因として保持し、KeyboardInterrupt等は呼び出し元へ再送出します。
自動再起動、Module単位の障害分離、データ排出は行いません。
Workerインスタンスは1回だけ実行できます。

## 状態と拡張

`worker.reports[stage_name]`はStageの状態・統計・エラー、
`.modules[module_name]`はModuleの状態・統計・エラーです。
呼出回数、PROCESSED/IDLE回数、処理時間合計・最大、Stageのスキップ予定数を保持します。
Stageのprocessedは最後まで呼び出した回数（FINISHEDを含む）、idleは途中でIDLEとなった回数です。
例外・停止で中断した回もexecutionsに含めます。実行中の読み出しは厳密に同期したスナップショットではありません。

独自Scheduleは`bind(stage, start)`でStage専用のConditionを作ります。
組み込みEventにはWorkerが通知を接続します。独自Scheduleは下記の契約を使います。

| Condition | 契約 |
|---|---|
| ready(now) | 待たずに実行可能か返す。入力データは消費しない（Eventは通知を消費） |
| next_check() | 次の確認時刻を単調時計で返す |
| after_run(finished_at) | Stage全体の終了後に予定を更新し、スキップ数を返す |

通知型の条件は状態を更新してからworker.wake(stage_name)で対象Stageを起こせます。
引数なしwake()は全Stageを起こします。通知待ちにはnext_checkで正の無限大を返します。
wakeはPeriodic/Pollingの予定時刻を早めません。通知はまとめられるため、条件自体を保持してください。

以前のWorker(modules)からWorker(stages)へ変更しています。
ModuleSpecのscheduleは削除し、Stageへ移しました。条件factoryはPollingへ移し、ModuleではなくStageを受け取ります。
旧`Event(condition, interval)`は`Polling(condition, interval)`へ変更してください。
新しい`Event()`は条件を取らない通知待ちです。

## SigBus用Module

`sigbus.module.Module`はSigBus用の処理基底クラスで、接続・資源管理・停止をまとめます。
Workerは開始前に`bind_stop_event(event)`を呼び、Moduleが所有するNodeとendpointへ停止を伝えます。
Workerは具象Moduleをimportせず、initialize/run_once/finalizeを持つオブジェクトを実行できます。
Spec一覧を受け取り、名前でPublisher／Subscriberを作成できます。bindingsは使いません。
APのwith acquireや手動checkoutを隠す処理コールバックは設けません。

```python
from sigbus.worker import RunResult
from sigbus.module import Module

class Filter(Module):
    def initialize(self):
        self.source = self.subscriber("signal")
        self.output = self.publisher("filtered")
        self.node = self.make_node(subscribers=(self.source,), publishers=(self.output,))

    def run_once(self):
        with self.node.acquire() as frames:
            frames[self.output].payload[...] = frames[self.source].payload * 2
        return RunResult.PROCESSED
```

複数周期で処理する場合はrun_once内でreserve/checkoutした領域を保持し、完成周期にcommit/completeします。
中止時はabortです。初期化・取得・完成判定はAPに見える形で記述します。
正常な有限生成を通知する前には`self.drain()`を呼び、True（端数も含めた公開完了）を確認します。
停止された場合はFalseです。タイムアウト再試行のループは不要です。

Worker.stopはModuleのNode／単独endpointの手動checkoutとdrainを中断できます。
停止に伴うInterruptedErrorはWorkerの異常として扱いません。
従来の単独endpointのwithをPollingWaitで待つ場合は、そのwaitにもstop_eventを設定してください。
finalizeは未確定の手動作業をabortし、正常時にdrain/close、停止時にdiscardします。
Node.closeが登録endpointも閉じるため、APがendpointとNodeの二重の終了リストを作る必要はありません。

## ソース構成

```text
sigbus/
├── module.py              # SigBus用Module・資源所有・終了処理
└── worker/
    ├── __init__.py        # 実行制御の公開API
    ├── core.py            # Workerの起動・並行実行・協調停止
    ├── stage.py           # ModuleSpec・Stage
    ├── scheduling.py      # Periodic / Event / Polling
    └── reporting.py       # RunResult・状態・統計
```

`from sigbus.module import Module`と`from sigbus.worker import Worker, Stage, ModuleSpec, RunResult`を使います。
汎用Module基底クラス・SigBusアダプター・別配布パッケージは設けません。
