# SigBus v1.0

同じCore APIからNumPy FrameをInProcess・SharedMemory・Fileで配信します。
**既定は満杯時に最古の非リースFrameを破棄するFIFO配信です。slotsは保持容量です。**
Bus/Publisher/Subscriberの既存メソッドとFrame(timestamp, payload)の生成方法は維持しています。
状態公開にはStateSpec、データごとの容量指定にはSpecのslotsを使えます。

## セットアップ

Python 3.13以上、NumPy 2.1以上。File/SharedMemoryはPOSIX向けです。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/python -m pytest -q
```

## 基本使用例

```python
import numpy as np
from sigbus import Bus, StreamSpec, InProcessTransport

transport = InProcessTransport(slots=10)
bus = Bus(transport)  # FIFOPolicy() と同じ
spec = StreamSpec('audio', (1024,), np.float32)
publisher = bus.publisher(spec)
subscriber = bus.subscriber(spec)
try:
    for i in range(10):
        with publisher.acquire(timestamp=i) as frame:
            frame.payload[:] = np.arange(1024, dtype=np.float32)
    for i in range(10):
        with subscriber.acquire() as frame:
            print(frame.timestamp, frame.payload.mean())  # 0〜9の順
finally:
    publisher.close()
    subscriber.close()
    transport.close()
```

書き込みのwith正常終了で1 Frame分を確定し、batch_size件に達するとslotを公開します。
batch_size=1（既定）は従来どおり毎回公開します。取得はwithに入る時点で行います。
subscriber.acquire()は各Subscriberの未読を取得し、正常・例外を問わずwith終了を読了と扱います。

## ストリームと状態をSpecで区別する

- `StreamSpec(..., slots=None)` は従来のストリームです。slots省略時はTransportの容量を使い、
  明示した場合はSpecを優先します。FIFO／Latest／Syncの読了規約は変わりません。
- `StateSpec(..., slots=2)` は現在値を保持します。既定は2面、1面または3面以上も指定できます。
  Transportの容量に関係なくStateSpecのslotsを使います。batch_sizeの指定はありません。
- 同じ名前のデータに接続するAPは、種別・shape・dtype・layout・実効slot数を一致させます。
  StreamSpecではbatch_sizeも一致が必要です。不一致はValueErrorです。
- 保存方式は独立です。InProcess／SharedMemory／Fileすべてで同じ状態公開を使えます。

```python
from sigbus import Bus, InProcessTransport, StateSpec, StreamSpec, StatePolicy, PollingWait

transport = InProcessTransport()
bus = Bus(transport)
sound_speed = StateSpec("sound_speed", (), "f8", slots=2)
beam = StreamSpec("beam", (1024,), "c16", slots=5)
pub = bus.publisher(sound_speed)  # StatePolicyを自動選択
sub = bus.subscriber(sound_speed)
try:
    with pub.acquire(timestamp=0) as frame:
        frame.payload[...] = 1500.0
    for _ in range(3):
        with sub.acquire() as frame:
            print(frame.payload.item(), frame.generation)  # 毎回1500.0、世代1
finally:
    sub.close()
    pub.close()
    transport.close()
```

StateSpecの取得は未読の消費ではありません。`acquire()`するたびにその時点の現在値を借り、
返却しても次の取得で同じ値を参照できます。取得中のpayloadはzero-copy・読み取り専用で、
更新が公開されても変化しません。次の取得で新しい現在値に切り替わります。
ビューはwith内だけ有効です。公開は読者不在でも保持し、後から接続した読者にも渡します。
closeはデータ削除ではありません。ただしInProcessTransport自体のclose後の保持は保証しません。

`frame.generation`は読み取りFrameの公開世代番号です。APが前回の番号と比較すれば、
状態参照を消費せずに更新を判別できます。同じtimestampで再公開しても世代は進みます。
書き込みFrameでは未公開なのでNoneです。StreamのFrameではslot公開単位の番号であり、
同じbatch内のFrameは同じ番号になります。remove後の再作成では番号はリセットされます。
更新待ち専用APIはありません。StatePolicyのPollingWaitは「更新」ではなく「取得可能」を待ちます。

状態での`has_frame()`は現在値を取得可能かを返し、読了後も通常Trueです。
**状態を `while sub.has_frame()` で読み切ろうとすると終了しません。**
未公開、同じendpointで取得中、または単一slotで書き込み中／書き込み待ちならFalseです。
状態の`acquire_batch()`はValueError、Publisherの`has_pending()`はFalse、
`flush()`は端数がないためno-opです（書き込みリース中は従来どおり拒否）。

### 状態のバッファ数と更新失敗

| slots | 更新・読み取り | 更新途中で失敗した場合 |
|---|---|---|
| 1 | 読者全員の返却後に上書き。書き込み中は読めない | 旧値には戻せず、次の正常公開まで状態は無効 |
| 2以上 | 現在値とは別の空きslotに書き、正常終了で現在値を切り替える | 直前の現在値を維持 |

単一slotでは、更新を待つ書き手がいる間は新規読者の取得を抑えます。
書き込みのタイムアウト・中断・例外時は待機登録を解除します。予約前の失敗では旧値を維持します。
複数slotでは取得中の旧世代も保護します。空きが尽きたら取得不可となり、勝手に参照を奪いません。
固定容量では、長時間保持したAPが書き手を止める可能性があります。計算単位で取得・返却してください。

待機を指定する場合は `StatePolicy(wait=PollingWait(timeout=5.0))` をendpointに渡します。
既定はImmediateWaitで、未公開・書き込み待ち・空き不足ではBlockingIOErrorの派生例外を返します。
StatePolicyはACK設定にも対応します。FIFO／Latest／SyncなどのPolicyをStateSpecに指定したり、
StatePolicyをStreamSpecに指定すると接続時にValueErrorです。
旧式のBus単位Policyを明示した場合もこの検証が適用されるため、混在するBusではendpoint単位で指定します。
異常終了したAPの参照や書き込み待ち登録は自動回収しません。ACKによるexcludeは待機登録の効力を
解除しますが取得中の領域は奪いません。回収が必要なら全AP停止後にremove(force=True)します。

## endpointごとにPolicyを指定する

PublisherとSubscriberは共通のPolicyクラスを使い、それぞれ設定します。
StreamSpecでは省略時はFIFOPolicy()、StateSpecではStatePolicy()です。新しいコードではBusにPolicyを設定する必要はありません。

```python
from sigbus import Bus, FIFOPolicy, LatestPolicy, PollingWait

bus = Bus(transport)
publisher = bus.publisher(spec, policy=FIFOPolicy(
    overflow="block", wait=PollingWait(timeout=5.0),
))
analysis = bus.subscriber(spec, policy=FIFOPolicy())
display = bus.subscriber(spec, policy=LatestPolicy())
```

同じStreamで解析は公開順、表示は最新だけを取得できます。表示の未読を理由に過去のslotを
保持しませんが、表示が取得中のslotは返却まで保護します。解析の未読位置は表示と独立です。
Latest読者がいる場合は、全FIFO読者が読了しても最新公開slotを次の公開まで保護します。
Latestを使うendpointには2 slots以上が必要です。最新保護によって利用可能な容量は減ります。

| Policy | Publisherでの動作 | Subscriberでの動作 |
|---|---|---|
| FIFOPolicy | 満杯時にdrop_oldestまたはblock | 保持中の未読を公開順に取得 |
| LatestPolicy | 最新公開slotを保護して更新。FIFO読者の未読が満杯ならblock | 未取得の最新slotから取得 |
| SyncPolicy | 同じgroupの同期対象が公開slotを読み終えるまで次の蓄積を開始しない | 公開時に参加していた同じgroupのデータを順に取得 |

overflowとno_readersはPublisherの設定です。Subscriber側の同名引数は保持規則を変更しません。
同じStreamの複数Publisherは配信方式・overflow・no_readers・Syncのgroupを一致させます。
不一致はValueErrorです。待機設定とACK設定はendpointごとに変えられます。
FIFOは全読者への配信で、ワークキューではありません。

### Syncグループとリース返却

```python
from sigbus import SyncPolicy, AckSettings, PollingWait

sync = SyncPolicy(
    group="analysis",
    no_readers="wait",
    wait=PollingWait(timeout=5.0),
    ack=AckSettings(interval=0.5, timeout=3.0, on_timeout="exclude"),
)
publisher = bus.publisher(spec, policy=sync)
analysis = bus.subscriber(spec, policy=sync)
display = bus.subscriber(spec, policy=LatestPolicy())
```

グループの範囲は同じTransport通信領域・同じStream内です。別プロセスでは同じ名前・設定の
Policyをそれぞれ作成します。Pythonインスタンスの共有は不要です。
Sync読者は同じgroupのSync Publisherからの公開を受け取ります。
1 StreamのPublisher規約は共通なので、異なるgroupや非Sync Publisherとの組み合わせは
接続時にValueErrorで拒否します。Subscriberが先に接続した場合もPublisher接続時に検証します。
独立した同期グループは別Streamで作ります。

同期対象はslotの公開時に確定します。公開後の途中参加は次の公開から対象です。
取得中に参加した読者も、公開時には対象になります。closeやACK期限切れによる脱落は待機対象を解除します。
FIFO読者も同じデータを読めますが同期対象ではなく、通常の容量保持にだけ参加します。
Latest読者も同期対象ではありません。どの読者も取得中のslotは保護されます。

リース返却を完了と扱います。例外終了も完了扱いで、計算の成功は保証しません。
バッチをFrame単位で読んだ場合は最後のFrameの返却まで待ちます。
コピー後にも計算完了を待たせたい場合は、その計算をwith内で終えてください。
受信ACK・完了ACK・独立した完了通知はありません。

### 全Policy・全Transport共通のACK

FIFOPolicy、LatestPolicy、SyncPolicyのいずれにも `ack=AckSettings(...)` を指定できます。
Publisher／Subscriber双方に対応し、InProcess・SharedMemory・Fileで同じ規約です。
既定の `ack=None` は監視なし・通信スレッドなしです。

- ACKは応答確認です。リース返却や計算の完了を意味しません。
- ACK有効のendpointごとに共通のバックグラウンドスレッドが応答時刻を更新します。
  アプリの取得APIは従来の同期withのままです。`publisher.ack()`／`subscriber.ack()`で明示応答もできます。
- intervalは送信間隔、timeoutは最後のACKからの期限（秒）です。`0 < interval < timeout` が必要です。
  接続時刻を最初の応答時刻とします。待機用のPollingWait.timeoutとは独立です。
- on_timeout="exclude": 配信・同期対象から外し、新しい取得・照会・flush・ACKを
  ParticipantExcludedErrorで拒否します。返却・closeは可能です。close後にendpointを作り直して再参加します。
- on_timeout="raise": 脱落させず保持・同期義務を残します。期限切れendpointの操作と
  Streamへの新規書き込み・flushはAckTimeoutErrorになります。PollingWaitはこの例外を再試行しません。
  対象をcloseして解消します。別プロセスが死んでcloseできない場合は全AP停止後にremove(force=True)で回収します。
- 期限切れのPublisherが持つリース中でない未公開端数はexclude時に破棄します。
  書き込みリース中なら返却まで保護し、返却時に公開せず解放して期限切れを通知します。
- **取得中の読み書きリースは、期限切れでも奪いません。** 同期待ちが解除されても
  pinが残れば容量不足になります。異常終了したプロセスのpinも自動回収しません。
- 通信ワーカーまたは通常のAPI操作が期限を判定します。全プロセスが停止中には監視も動きません。
  OSロックやI/Oに時間がかかれば判定は遅れます。厳密な実時間保証はありません。
- バックグラウンド通信のエラーは次のendpoint操作で通知します。closeはワーカーを停止・joinします。
  リース中のcloseは拒否し、ACK送信を継続します。

自動ACKが続くことは計算の進行を保証しません。GILを長時間解放しない処理やOS停止では
通信スレッドも遅れるため、計算からの実行分離が必要なら計算を別プロセスへ渡してください。
共有メモリでもACKの管理にはファイルロックとJSON更新を使い、短いintervalにはコストがあります。

### 待機と読者不在

- ImmediateWait（既定）: 新着なし・空きなし・同期待ちではBlockingIOError。
- PollingWait: 取得時のBlockingIOErrorだけを再試行。timeoutはwith開始からの単調時計で計測し、
  満了でTimeoutError。Noneは無期限、0は一度だけ試行します。
  threading.Eventをstop_eventに指定するとInterruptedErrorで中断できます。
- intervalは失敗後の待ち時間で、配信周期ではありません。待機中は新たなslotやlockを保持しませんが、
  Publisherが蓄積中の端数slotは保持します。AP処理・公開・返却時の例外は再試行しません。
- 同じスレッドでProducerが満杯待ちするとConsumerへ到達できません。
  並行処理は別スレッド／別プロセスで動かしてください。

| no_readers | Publisherの動作 |
|---|---|
| hold | FIFOでは容量まで保持。Sync／Latestでは非対応 |
| wait | 対象読者が0なら書き込み予約不可。Syncでは同じgroupのSync読者を数える |
| proceed | 読者なしでも進行。FIFOは対象のない公開を保持せず、Latestは最新値を残す |

FIFOの既定はhold、Sync／Latestの既定はproceedです。
waitの判定は予約時で、予約後の切断は取得済み書き込みを取り消しません。
FIFOの途中参加はまだ保持中の公開データから、Latestは現在の最新値から読み始めます。

### Policy拡張と旧API

Policyの共通契約は `open_stream(adapter, spec, mode)` と `acquire(factory)` です。
独自Policyのコンストラクターに共通制約はなく、Busは上記のメソッドだけを呼びます。
独自の配信規約にはTransport内部の対応も必要です。

既存コードの移行用に `Bus(transport, policy=...)` は残しています。
endpointへの明示指定が優先され、省略時だけ旧Bus設定を使います。
旧ReadCompletePolicyも互換用に残し、全読者の読了待ち・現在世代への途中参加という従来の意味を維持します。
新しいグループ同期にはSyncPolicyを使ってください。
旧ImmediatePolicy／PollingPolicyは提供しません。

## Transportと資源管理

- InProcessTransport: 同じインスタンスを共有するBus同士が通信。NumPy配列とRLock。
- SharedMemoryTransport(namespace, slots=..., directory=...): 同じnamespaceと保存先で通信。
  標準SharedMemory上のビューとflock、JSONメタデータ。Python 3.13のtrack=Falseを使用。
- FileTransport(directory, slots=...): 同じ保存先で通信。Stream名そのままのサブディレクトリに
  slot-N.npy、metadata.json、lockを保存。payloadはmemmap。

SharedMemory/FileはプロセスごとにTransportを作成します。forkで継承したendpointは使用せず、
spawnまたは独立起動で同じStreamSpec・slots・配信規約を指定します。
TransportとAdapterは同義の実装で、各Transportには同名のAdapter別名があります。

closeはローカル資源の解放のみで保存データを削除しません。先に自分のendpointを閉じます。
File/SharedMemoryの削除は全プロセスのendpointを閉じてから `transport.remove(spec)`、次にcloseです。
lockファイルとディレクトリは、別々のlockが並存する競合を防ぐため残します。
Fileは固定容量で、全世代のアーカイブではありません。保持中の未読FIFOデータは正常再接続後にも取得できます。
稼働中の.npyを直接読むとpinされないため、Subscriberから読み出してください。

保存形式はversion 7です（Specの種別と状態更新の書き込み待ち情報を追加）。
version 6以前は自動移行しません。既存データが必要なら旧実装で退避し、新しい保存先／namespaceを使用してください。payloadファイルも先頭にbatch軸を持つため、旧versionの自動移行は行いません。
旧データを必要に応じて旧実装で読み出して退避し、新しい保存先／namespaceを使用してください。
旧形式は明示的に拒否します。旧実装との同時利用も非対応です。

## 保証と制約

- timestampは整数で、単位・単調性は規定しません。FIFOの順序は公開完了の世代順です。
- payload・slice・viewはwith内だけ有効。後で使う場合はcopyしてください。
  読み取り専用フラグは誤操作防止であり、既存の別名ビューの強制失効はできません。
- 入出力のpayloadビュー取得はzero-copyですが、データ代入・JSON管理・File flushにはコストがあります。
- 書き込みslotの初期値は未定義。毎回全要素を代入し、Frame属性そのものは置き換えないでください。
- FIFOの書き込み失敗はその1件を未確定とし、他の未読Frameと同じslotの確定済み端数を維持します。
  確定済み端数がなければ予約を解放します。
  drop_oldestの破棄は書き込み予約時に確定し、その後の書き込みが失敗しても破棄済みFrameは復元しません。最新値は直前の最新Frameを維持します。
  読了同期は次の予約時に旧世代を取り下げ、失敗後は未公開状態です。特にslots=1の上書きはコピーなしに復元できません。
- row_majorのみ。batch_sizeは正の整数で、shapeはAPが扱う1 Frameのshapeです。スカラーshapeも対応、空配列は非対応。
  固定サイズのNumPy dtype向けで、object、root subarray、.npyに忠実に表現できない型は対象外です。
- 同じendpointでの同時・入れ子取得は拒否します。同期APIのみでasync withは未実装です。
- 信頼できる同一ユーザーのローカル利用向け。NFS、ハードリアルタイム、電源断・I/O障害への完全耐久性は非対応です。

## 検証と次の評価

tests/test_fifo.pyは既定FIFO、10件連続公開→10件取得、満杯、返却、遅い読者、途中参加、close、
例外、公開完了順、slots=1、Polling、読者不在規則、別プロセス／再オープン、Policy拡張契約を検証します。
既存の最新値・読了同期・待機・zero-copy・複数プロセス同時書き込みのテストも維持しています。

実APでは遅延（p95/p99）、未読滞留、slot数、読者保持時間、停止手順を評価してください。
ワークキュー、履歴再生、任意のドロップ規則、異常終了の自動回収は未実装です。

検証環境: macOS / Python 3.13。最新の実行結果は以下のテストコマンドで確認できます。
OS共有メモリを利用可能な実行環境で確認しています。Linuxでの実行と性能計測は未実施です。
endpoint別Policy・Sync・ACKの追加検証内容は `tests/test_endpoint_policies.py` を参照してください。


## 満杯時の既定動作

`Bus(transport)` は `FIFOPolicy(overflow="drop_oldest")` です。
空きslotがあれば優先して使用し、満杯ならリースされていない公開Frameのうち最古を破棄します。
最古がリース中なら、それを保護して次に古い非リースFrameを選びます。
全Subscriberの当該世代の未読情報も取り除き、各読者は残る世代を公開順に読みます。
したがって、slot=5で0〜9を先に公開すると5〜9の5件を取得できます。
同じコードで10回読み出せるわけではありません。6回目の読み出しは未読なしになります。

未読を捨てない運用は `FIFOPolicy(overflow="block", wait=PollingWait(...))` などで明示します。
block自体は空きslotの条件であり、即時に返すか待つかはwaitが決めます。
既定でも全slotが読者／書き込み者にリースされていれば進行できません。

エラーはBlockingIOErrorの派生型として区別します。
- `NoWritableSlot`: 安全な書き込み先がない。Stream名・保護理由・対処をメッセージに含めます。
- `NoFrameAvailable`: そのSubscriberが取得できる未読Frameがない。

既存のBlockingIOErrorハンドラーとPollingWaitによる再試行は互換です。
リース保護やデータのない読み出しを無視して「常に成功する」変更はしていません。

## 未読Frameの確認

`subscriber.has_frame()` は、そのSubscriberが現在取得可能な未読Frameの有無をboolで返します。
FIFO・読了同期・最新値それぞれの取得規則に従い、PollingWaitでも新規Frameを待ちません。
読了位置や参照数を変更せず、ACK期限切れがなければ永続メタデータも書き換えません（照会時のロック待ちはあり得ます）。
ACK期限切れの判定・脱落処理は照会時にも行われ、その場合はメタデータを更新します。
同じendpointがリース中ならFalse、close済みならRuntimeErrorです。

```python
while subscriber.has_frame():
    with subscriber.acquire() as frame:
        print(frame.timestamp, frame.payload.mean())
```

公開処理が終わった後に保持データを読み切る用途に使えます。
照会は予約ではないため、並行する公開・破棄・同じendpointの操作によって取得時には状態が変わり得ます。
並行運用ではacquireの例外／待機処理も必要です。Publisher向けの照会APIはまだ追加していません。


## slot内のバッチBuffer

```python
spec = StreamSpec('audio', shape=(10, 10), dtype=np.float32, batch_size=4)
publisher = bus.publisher(spec)
subscriber = bus.subscriber(spec)
try:
    for i in range(6):
        with publisher.acquire(timestamp=i) as frame:
            frame.payload[:] = i  # 常にshape=(10, 10)
    # 最初の4件は自動公開済み、残り2件は未公開。
    if publisher.has_pending():
        publisher.flush()         # 残り2件を公開。ゼロ埋めしない。
    while subscriber.has_frame():
        with subscriber.acquire() as frame:
            process(frame)        # ここでもshape=(10, 10)
finally:
    publisher.close()             # 残った未公開の端数は破棄
    subscriber.close()
```

Buffer内にFrameオブジェクトは保持しません。各slotはC連続配列 `(batch_size, *shape)` を持ち、
Transportが `index * prod(shape) * dtype.itemsize` の位置から元のshapeのndarrayビューを作ります。
スカラーshape `()` も0次元ndarrayとして取得できます。timestampは各確定要素に対応して内部管理します。
Fileの.npyもbatch軸を含む形です。有効件数・timestamp列・読者位置はメタデータで管理します。バッチ取得では有効部分とtimestamp列をAPへ返します。

- slotはPublisherごとに確保します。異なるPublisherの端数を混ぜません。
  複数Publisherが端数を持つと、それぞれ1 slotを占有するため、必要なslots数に注意してください。
- 正常返却で1件確定。例外では位置を進めず、次回同じ位置を貸し出します。
  batch_size件が揃うまで読み出し不可です。バッチの順序は蓄積開始順ではなく公開完了順です。
- `has_pending()` は自分の確定済み未公開データがあればTrue。書き込み中の未確定1件は数えません。
  それ以前の端数がある場合は書き込みリース中でもTrueです。closedならRuntimeError。
- `flush()` は自分の端数だけを公開します。0件なら何もしません。
  書き込みリース中は拒否します。公開後はhas_pending()がFalseになります。
  closeはflushを代行せず、自分の未公開端数だけを破棄します。
- `has_frame()` は当該Subscriberに取得可能な公開済みFrameがあるかを返します。
  未公開の端数は対象外。照会自体は位置や件数を変更しません。
- FIFOは公開slot内の有効件数を先頭から順に取得。読者ごとの位置は独立です。
  blockでは全対象読者が最後まで返却／closeするまでslotを再利用しません。
  drop_oldestの破棄はslot単位で、そのslot内の残りの未読Frameすべてが破棄対象です。
  1 Frameでもリース中ならslot全体を保護します。
- 読了同期もslot内を順に取得し、全対象読者のslot全体の読了を待ってから次slotの蓄積を開始します。
- 最新値は最新公開slot内の最後の1 Frameだけを取得します。
  例えばbatch_size=4なら0〜3公開後は3、4〜6の蓄積中は新着なし、7の公開後は7を取得します。
  flushした端数でも最後の有効Frameだけを取得します。

close後に未公開端数は復元されません。ACKによるexclude時にはリース中でない端数を回収します。取得中の所有slotは自動回収しません。
Fileのmemmap.flush()は、自動公開またはpublisher.flush()による端数公開の直前に、対象slotだけに実行します。
蓄積途中・空のflush・書き込み中断・端数を破棄するcloseでは明示的なmemmap.flush()を行いません。
例えばslots=10、batch_size=100なら、100件蓄積して自動公開するまでのflush呼び出しは対象slotの1回です。
OSが未公開データを先にファイルへ書き戻すことはあり、明示flushしないことは非永続化を保証しません。

自動公開時のflushが失敗した場合、公開せず、最後の1件を未確定としてリースを解放します。
それ以前の確定済み端数は維持し、最後の1件の再書き込み、または端数の明示flushが可能です。
publisher.flush()が失敗した場合も端数は未公開のまま維持され、再試行できます。
メタデータはpayloadのflush成功後に公開状態へ更新します。電源断やメタデータ書き込み障害まで含む
完全なトランザクション耐久性は提供しません。各取得・返却時のJSON更新は維持しています。ACK有効時は応答ごとのJSON更新も行います。

## SharedMemoryの強制回収

関連APをすべて停止した後、removeにforce=Trueを指定できます。
旧形式・StreamSpec不一致・異常終了で残ったendpoint情報を検証せず回収します。

```python
transport = SharedMemoryTransport(namespace='default')
try:
    transport.remove('audio', force=True)
    # StreamSpecを持っている場合は transport.remove(spec, force=True) でも同じ。
finally:
    transport.close()
```

文字列はOSのpsm_...という共有メモリ名ではなく、指定namespace内のStream名です。
カスタムdirectoryで作成した場合は同じdirectoryを指定してください。slots数やdtype/shapeは不要です。
メタデータのshared_memory名で本体をremoveしてからメタデータを削除し、lockファイルは残します。
本体が既に消えていればメタデータのみ回収し、対象がなければ何もしません。
壊れたJSONや共有メモリ名が特定できないメタデータは、誤回収を避けるためエラーにします。

force省略時の既存チェックは維持します。forceはプロセスを停止せず、他プロセスの既存マッピングも
強制失効しません。必ず利用を停止してから実行し、古いendpointやFrameは再使用しないでください。
同じTransportの対象Streamキャッシュは破棄され、次の接続で新しく作成できます。


## Streamの削除と例外

全Transport共通で `transport.remove("audio", force=False)` を使用します。
StreamSpecを渡してもStream名だけを使います。対象がなければ新規作成せず何もしません。
正常時も旧形式の回収時も、データ形式・shape・dtype一致を削除条件にはしません。
通常はendpoint登録が1件でもあればStreamInUseErrorをraiseし、何も削除しません。
force=Trueは残存登録を無視しますが、破損した削除対象の特定情報や権限エラーを無視しません。
Stream削除APIはremoveのみです。unlinkは提供しません。

| 例外 | 意味 |
|---|---|
| StreamInUseError | remove時に接続中のendpointがあり、削除を拒否 |
| StreamRemovedError | 接続していたStreamが削除された、または同名で再作成された |
| StreamCorruptedError | JSON・管理構造・payloadの構造が壊れている／メタデータはあるがpayloadがない |
| StreamPermissionError | Stream操作に必要なOS・ファイル権限がない |

Policy／StreamSpecの不一致はValueError、その他のI/O障害は元のOSErrorです。
例外メッセージにはStream名と操作を含め、変換元の例外があればcauseに保持します。
PollingWaitが再試行するのは取得不可のBlockingIOErrorのみで、上記4種類はAPへ返します。

永続Transportでは接続時の一意IDとメタデータを照合します。古いendpointは同名Streamの再作成後も
新しいStreamへ乗り換えず、次の取得・照会・flush・返却でStreamRemovedErrorになります。
InProcessでも削除済みstorageを無効化し、旧endpointを新しいStreamから分離します。
remove後のendpoint.close()は成功します。正常なwith終了で削除を検出したら例外を返し、
APが既に例外を出している場合はその例外を返却処理で上書きしません。

removeは取得済みNumPyビューを強制失効させたり、他プロセスを停止したりしません。
ライブラリのAPIを呼ばずにpayloadを直接参照している間は削除通知できません。
Fileや共有メモリの既存マッピングはcloseまで保持されることがあり、OS領域の最終解放も
残る利用者のcloseを待つ場合があります。forceは通常AP停止後の残存資源回収に使ってください。

`tests/test_remove.py` では3 Transportの削除拒否・強制削除・照会・リース返却、Polling中断、
別プロセスでの削除／再作成、旧形式削除、破損・権限・一般I/O障害の区別を検証します。


## バッチ一括取得と軸を指定した連結

Subscriberの `acquire_batch()` は、1つの公開済みslotの未読部分を
`Batch(timestamps, payload)` として取得します。slotを跨いでデータを集めません。
payloadのshapeは `(有効件数, *StreamSpec.shape)`、timestampsは各Frameに対応する整数のtupleです。

```python
spec = StreamSpec('audio', shape=(512, 8), dtype=np.float32, batch_size=2)
publisher = bus.publisher(spec)
subscriber = bus.subscriber(spec)
# Publisherは従来どおり512 sample × 8 channelを2回書き込む。
# 2件揃うと自動公開される。
with subscriber.acquire_batch() as batch:
    samples = batch.concatenate()  # axis=0が既定。shape=(1024, 8)、zero-copy
    spectrum = np.fft.rfft(samples, axis=0)
```

`batch.concatenate(axis=0)` のaxisは、バッチ軸を除いた元のFrame内の軸番号です。
負の軸番号も受け付け、元のFrameの次元数で正規化します。

| 元のFrame shape | axis | 2件連結後 | コピー |
|---|---|---|---|
| (512,) | 0 または -1 | (1024,) | なし |
| (512, 8) | 0 | (1024, 8) | なし |
| (8, 512) | 1 または -1 | (8, 1024) | あり |

正規化したaxisが0ならC連続のバッチ軸とFrameの先頭軸をreshapeでまとめます。
axisが1以降ならコピーして連結します。有効件数が1件でもこの規則は同じです。
payloadと連結結果は読み取り専用です。zero-copyのpayload・連結結果・派生ビューは
with内だけ有効で、後で使用する場合はcopyしてください。他軸の連結結果は独立したコピーです。
スカラーFrameはバッチ取得できますが、連結する軸がないためconcatenateはValueErrorになります。
範囲外のaxisはValueError、整数でないaxisはTypeErrorになります。

- FIFO・読了同期では、同じSubscriberが通常のacquire()で既に読んだFrameを除き、
  選択したslotの未読部分だけを返します。通常取得とバッチ取得で読了位置を共有します。
- LatestPolicyでは最新公開slotの全有効Frameを返します。
  通常のacquire()は従来どおり最後の1 Frameだけを返し、そのslotを読了にします。
- with中はslot全体を保護し、正常・例外を問わずwith終了時に取得した部分を一括で読了にします。
  他のSubscriberの読了位置は変わりません。同じendpointでの入れ子・同時取得は禁止です。
- has_frame()とPolicyの待機設定は通常取得と共通です。
- flush()で端数を公開した場合は、その有効部分だけを返します。
  必要サイズへの調整はAPの責任です。常に満杯のバッチを必要とするPublisherは
  端数をflushせず、満杯時の自動公開だけを使用できます。close時の端数は破棄されます。
- timestampの単位や時間的連続性はSigBusが保証しません。
  連結してもfsは変わらず、欠落や異なる時刻間隔を補正する処理は行いません。
- 分割APIは提供しません。1024 sampleを512 sampleずつ処理する場合は、
  with内でsamples[:512]とsamples[512:]のようなzero-copyスライスを使用してください。

バッチ取得は保存形式version 6を使用し、通常のacquire()の戻り値や動作は変更しません。


### FileTransportのストリーム名と保存先

FileTransportでは `FileTransport(directory="/data")` とStream名 `audio` に対し、
`/data/audio/metadata.json`、`/data/audio/slot-0.npy`、`/data/audio/lock` を使用します。
名前は `[a-z0-9][a-z0-9_-]{0,62}`（1〜63文字）に制限します。
大文字・日本語・空白・ドット・パス区切りは使用できません。
接続とremoveの両方で検証し、自動的な小文字化や置換はしません。
この制約はFileTransport固有で、InProcessとSharedMemoryの名前・保存先は変更しません。

旧SHA-256名のディレクトリが存在する場合、接続・removeはValueErrorになります。
旧データは自動移動・削除しません。関連プロセスをすべて停止し、metadata.jsonの
spec.nameを確認したうえで、そのハッシュディレクトリを対応する有効なStream名へ
リネームするか、新しい保存先を使用してください。移動先が既に存在する場合は
上書き・統合しないでください。ディレクトリ名の変更だけでは旧versionから移行できません。現在の形式はversion 6です。
旧名が新しい制約を満たさない場合は、旧実装で必要データを退避して新しい名前で作成してください。
