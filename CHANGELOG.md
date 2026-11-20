# Changelog

## 2.0.0 — 2026-11-20

WorkerとModuleを含むSigBus 2.0.0です。

### Worker / Module

- Moduleの契約、Stageの構成、実行制御、レポート、スケジュールを別ファイルに整理。
- SigBus用Moduleをsigbus.moduleへ、実行制御をsigbus.workerへ配置。
- ModuleによるNode・単独endpointの所有、正常終了時のdrain、停止時の破棄・解放。
- Eventを通知駆動に変更し、条件確認による起動をPollingとして分離。
- sigbus-workerの別配布、汎用Module基底クラスとアダプター層を廃止。

### SigBus

- Nodeで複数endpointの予約・取得・確定・返却と資源管理を統合。
- Nodeと単独endpointにreserve / checkout / commit / complete / abortを提供。
- バックグラウンド送信と、確定済み端数を含むdrain・停止可能な待機を追加。
- StreamSpecのrate_hzを必須化し、NodeのFrame/Batch取得時にレート整合性を検証。
- endpointとNodeでBatchの取得・書き込みに対応。書き込み時刻は明示指定。
- NodeとModuleを使用したビーム形成の実行例とテストを追加。

### APIの整理と破壊的変更

- File/SharedMemoryの保存形式をversion 8へ更新。旧形式の自動移行はありません。
- 旧StreamSpecにはrate_hzを追加してください。
- 旧Event(condition, interval)はPolling(condition, interval)へ変更してください。
- UpdateTracker、StreamReady、Periodic.every、Bus.node、ReadCompletePolicyを削除。
- 破棄終了はdiscard()に統一し、close(drain=False)を削除。
- Transportの旧Adapter別名を削除。
- Node.flush()はendpointと共通の出力操作として保持。

詳細は[移行手順](docs/migration-v2.md)を参照してください。
