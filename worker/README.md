# worker

同一プロセス内で信号処理Moduleを動かす、SigBusから独立したモジュールです。

- Module：処理単位。初期化・1回分の処理・終了を実装します。
- Stage：逐次実行とスケジュールの単位。1つ以上のModuleを登録順に実行します。
- Worker：同一プロセスへの配置単位。Stageごとに専用スレッドを起動します。

Stage間は並行実行、Stage内は逐次実行です。SigBus本体は変更しません。
プロセス生成・Control Plane・パラメータ配送は今回の対象外です。

Python 3.13以上。配布名は`sigbus-worker`、import名は`worker`です。外部依存はありません。
SigBusを使うアプリケーションは別途SigBusをインストールしてください。

```sh
python -m pip install -e ./worker
python -m pytest -q -c worker/pyproject.toml worker/tests
```

## 配置と実行例

```python
from worker import ModuleSpec, Periodic, RunResult, Stage, Worker

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

## Periodic / Event

スケジュールはModuleごとではなくStageごとに指定します。

- `Periodic(rate_hz)`：全Stageの初期化完了後の共通基準時刻から実行予定を計算します。
  初回は即時実行可能。Stage全体の処理終了時に過ぎた予定を飛ばします。
  処理時間を周期へ加算せず、単調時計を使います。
- `Event(condition, interval=0.001)`：初期化後に`condition(stage)`を呼び、
  引数なしの判定関数を作ります。判定は待機・データ消費をせず、実行可能ならTrueを返します。
  確認後とStage実行後のいずれもinterval秒以上空けて確認します。

Eventは通知件数を蓄積せず、条件が継続して成立していれば再び実行します。
実行条件の決定はアプリケーションの責務です。

## SigBusとの接続

```python
from worker import Event, ModuleSpec, RunResult, Stage, Worker
from worker.adapters.sigbus import StreamReady
from sigbus import Bus, FileTransport, NoFrameAvailable, StreamSpec

class Analysis:
    def initialize(self):
        self.transport = FileTransport(directory="/tmp/worker-demo")
        self.subscriber = Bus(self.transport).subscriber(
            StreamSpec("input", (1024,), "f4"))

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
          Event(lambda stage: StreamReady(stage.module("analysis").subscriber)))
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

StreamReadyは公開APIのhas_frame()だけを呼びます。結果は予約ではなく、
確認後に取得できない場合はModuleが扱います。Stateの更新検出には使用できません。
Subscriberの種別は公開APIで判定できないため、自動検証はしません。

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
Workerは具体的なスケジュール型に依存しません。

| Condition | 契約 |
|---|---|
| ready(now) | 待たず、消費せず、実行可能か返す |
| next_check() | 次の確認時刻を単調時計で返す |
| after_run(finished_at) | Stage全体の終了後に予定を更新し、スキップ数を返す |

通知型の条件は状態を更新してからworker.wake(stage_name)で対象Stageを起こせます。
引数なしwake()は全Stageを起こします。通知待ちにはnext_checkで正の無限大を返します。
wakeはPeriodic/Eventの予定時刻を早めません。通知はまとめられるため、条件自体を保持してください。

以前のWorker(modules)からWorker(stages)へ変更しています。
ModuleSpecのscheduleは削除し、Stageへ移しました。EventのfactoryもModuleではなくStageを受け取ります。
