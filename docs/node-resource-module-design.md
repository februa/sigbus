# Node・endpoint・Moduleの契約

## 取得条件はendpointの通信規約

StatePolicy()は毎回現在値を参照する。FIFOは未読を順番に取得する。
今回の例の音速・Steering・Weights・Displayは周期駆動と現在値参照で十分である。
値比較による公開抑制と更新取得規約の追加は保留する。更新追跡用の補助APIは設けない。
複数周期で計算する値はAPが完成時にcommitし、通信側で計算完了や値変化を推測しない。

## 資源管理の自動生成と移管

Publisher／Subscriber生成時にEndpointResourcesも内部で自動生成する。戻り値はendpointのみ。
Node未使用時はendpointが所有し、endpoint.close()が管理オブジェクトへ終了処理を委譲する。
Node登録時は同じ管理オブジェクトをNodeへ移管する。APに生成・ペア設定・別リスト管理を要求しない。
使用中・二重登録を拒否し、登録失敗時には元の所有者を維持する。
Node.close()は登録endpointの資源も閉じる。閉じたendpointを再利用する契約にはしない。

## drain

Node.drain(grace_period=0, timeout=None, stop_event=None)は確定済み端数も公開する。
猶予時間中は通常のバッチ公開を許し、満了後に残った端数をflushする。猶予満了は正常動作。
既定では猶予なしでflushする。未確定のAP書き込みは公開しない。
公開成功までは同じ出力を保持して送信リトライする。
完了時True、全体の待機期限または停止要求に達した場合Falseを返す。未完了の出力は保持する。
待機打切りをTimeoutErrorやwarningにしない。Stream削除など継続不能な失敗は例外とする。
Node.flush()はPublisherと共通の出力操作として残し、登録した全出力を対象に猶予なしのdrainとする。discard()は未送信出力を明示的に破棄する終端操作。
closeは既定でdrainしてから資源を閉じる。期限内に終わらなければFalseを返し、資源を保持する。
取得中のdrainは拒否する。drain中は別スレッドの取得を猶予期間に限り許し、flush開始までに進行中の取得を待つ。
完了後に開始される新規取得は別の処理であり、drainの完了保証には含めない。

## Module

Workerはsigbus.workerに配置し、処理ライフサイクルの実行とスケジュールを担当する。
SigBus用Moduleはsigbus.moduleに配置し、Workerとの相互importやアダプター層を設けない。
Workerはライフサイクルと停止接続の契約を呼び、通信の実装には依存しない。
ModuleはNode/未移管endpointの所有、Worker停止とNode待機の接続、正常時のdrain、
異常時の明示破棄と資源解放を提供する。Node未使用のendpointも後始末できる。
APはinitializeとrun_once/step内に接続とwith node.acquire()を明示する。
暗黙のacquire、process(frames)呼び出し、bindingsは導入しない。
Spec一覧とModule内の名前による接続を維持する。

## 手動操作を基本とするNodeの状態遷移

reserve(target="all", timestamp=None)で対象を指定する。targetは"subscribe"、"publish"、"all"。
予約要求はNodeへ登録され、checkout(timeout=None)で各endpointの予約を順次成立させ、
全対象が揃ったところで領域をAPへ渡す。checkoutはwithを使わない即時実行APIである。
取得した領域は周期・関数呼出しをまたいで保持できる。既に取得中の側への重複予約は拒否する。
購読側をcheckoutしてから配信側を追加でcheckoutできる。戻り値は今回対象のendpoint→Frameのmapping。
checkoutの期限切れでは成立済み予約を保持する。同じcheckoutを再試行するかabortで解除する。

commit()は取得中の出力一式を確定してsendへ渡し、APの出力アクセスを終了する。
complete()は取得中の入力の使用完了を通知し、読了・返却する。
abort()は未完了の予約・取得を取り消す。入力は未読のまま、出力は未確定分だけを破棄する。
commit済み出力はabortでは取り消さない。送信待ちの明示破棄は終端操作discard()を使う。
出力のないcommit／入力のないcompleteは何もしない。資源終了前には保持中の取得を完了または中止する。

with node.acquire()の開始はreserve→checkout、正常終了はcommit→complete。
取得途中の失敗はabort、AP処理中の例外はcomplete→abortを呼ぶ。
手動APIと別の予約・確定処理を実装しない。with内からの手動操作や手動取得中のwith開始は拒否する。
Node.close/discardはcheckout待ちを中断するが、APが使用中の領域を勝手に解放しない。

音速・Steering・Weightsは今回の例では周期的な現在値参照・公開を使う。
変更検出はこの例の駆動条件に使用しない。

## 単独endpointとNodeの同じ手動操作

Publisher/Subscriberにもreserve・checkout・commit・complete・abort・drainを提供する。
単独endpointは対象が自明なのでreserveに対象指定は不要で、checkoutはFrameを返す。
Nodeはreserveで対象群を選び、checkoutでendpoint→Frameのmappingを返す。
両者は共通のライフサイクル実装を使う。単独endpointの利用にAPがNodeを生成する必要はない。
手動利用中のendpointはNodeへの登録を拒否する。手動作業と送信が完了したら同じ資源を移管できる。

単独endpointのwith acquireは同期公開・wait policyによる待機を行う。
新しい手動commitとNodeのcommitはバックグラウンドsendへ引き渡す。
単独endpointのwithと手動操作の同時使用は拒否する。手動APIの状態管理を別実装にしない。

## StreamのレートとBatch取得（新契約）

StreamSpecのrate_hzは必須の正の有限値（Frame/秒）。旧Specとの互換は設けない。
StateSpecはrateを持たず、現在値参照のまま。保存形式はversion 8へ変更する。
Node登録時に、登録Stream群がFrame取得またはBatch取得の少なくとも一方で整合するか検証する。
Frame取得はrate_hz一致、Batch取得はbatch_size/rate_hz一致。取得開始時に選択モードを再検証する。
Stateはこの時間幅比較から除外する。時刻同期・位相・欠落の対応はModule/Workerが担当する。

endpoint/Nodeともacquire/acquire_batch、checkout/checkout_batchを提供する。
BatchモードのStream購読は1 slotの未読部分をBatchとして取得する。Stateは両モードでFrameを返す。
BatchモードのStream配信は1 slot全体をBatchとして取得し、一括commitする。
書き込みにはbatch_size件のtimestampsを明示する。自動補間・rateからの時刻生成は行わない。
複数Publisherの場合はNode.reserve/acquire_batchのtimestampsにendpoint→timestamp列のmappingを指定できる。
確定済み端数があるPublisherのBatch書き込みは拒否し、先にdrain/flushする。
端数・途中まで読了したBatchも取得でき、有効件数とtimestampを返す。時間整列や不足補充はしない。
