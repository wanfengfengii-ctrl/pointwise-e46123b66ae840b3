# 临汀观测湖仓

临汀环境资料中心保存监测站的日批。白班核账员冻结指定批修订并核对行数、缺测数和非空整数合计。档案接收方报告缺行后，值班员按发布及行号登记补送，接班人接手指定范围，最后由接收方领取固定实收清单。

## 运行

使用 Python 3.11 标准库。在项目根目录运行以下命令。

```sh
python3 -B check_project.py
python3 -B -m lake.audit.workday demo
```

演示用独立临时目录和 `samples/workday.json`。它实际冻结 `5, null, 11`，再用两行补送样例演示回包隐藏、换班、重开查证和签结。正常回归入口由运行环境提供，套件位于 `tests` 包。

## 一天的操作

以下示例使用同一目录。CLI 接受一个 JSON 对象作为每条业务命令的参数，并以 JSON 返回结果。初始化在重复调用时保留已有数据。

```sh
python3 -B -m lake.audit.workday --workspace ./example-workday init
python3 -B -m lake.audit.workday --workspace ./example-workday ingest '{"station":"river","day":"2026-04-17","revision":"r1","values":[5,11]}'
python3 -B -m lake.audit.workday --workspace ./example-workday freeze '{"partitions":[["east",[["river","2026-04-17","r1"]]]]}'
python3 -B -m lake.audit.workday --workspace ./example-workday read-release '{"release_id":1,"partition":"east"}'
python3 -B -m lake.audit.workday --workspace ./example-workday open-job '{"job_id":"A","target":"archive","release_id":1,"bounds":[1,2]}'
python3 -B -m lake.audit.workday --workspace ./example-workday send '{"job_id":"A","line":1,"attempt_id":"A-1","hide_reply":true}'
python3 -B -m lake.audit.workday --workspace ./example-workday handoff '{"from_job":"A","to_job":"B","bounds":[1,2]}'
python3 -B -m lake.audit.workday --workspace ./example-workday send '{"job_id":"B","line":2,"attempt_id":"B-2"}'
python3 -B -m lake.audit.workday --workspace ./example-workday reconcile '{"attempt_id":"A-1"}'
python3 -B -m lake.audit.workday --workspace ./example-workday issue-statement '{"target":"archive","release_id":1,"bounds":[1,2]}'
python3 -B -m lake.audit.workday --workspace ./example-workday read-statement '{"statement_id":1}'
```

发布号和清单号由中心分配。示例中的 `1` 适用于新目录，既有库应使用命令返回的实际编号。`job-status` 接受 `job_id`，返回该作业当前持有的行及其待送、未决或已收状态。`receiver-log` 接受 `target`，返回独立目标的实际收件顺序、明细和回执。

`send` 的 `event` 可指定 `received`、`pending` 或 `not_received`，默认 `received`。`hide_reply` 模拟目标处理后中心未收到回包。目标的延迟终态由 `receiver-outcome` 登记，参数为 `attempt_id` 和 `status`，终态取 `received` 或 `not_received`。`receiver-lookup` 接受 `attempt_id` 和布尔值 `available`，用于模拟该尝试暂不可查证。这两个命令记录目标事件，中心仍须调用 `reconcile`。

## 数据口径和公开 Python 接口

- `ingest` 接受非空站点及修订标识、`YYYY-MM-DD` 日期和有序读值数组。读值为整数或 `None`，零和负数是实测，布尔值、浮点数和数字字符串无效。重复修订键拒绝，保留首次录入。
- `freeze` 接受有序的 `[分区名, 源批清单]` 项，每个源批使用 `[站点, 日期, 修订]`。分区名唯一，同一源批键在一次发布中只列一次。批内原位保留，冻结行号按分区、批、原位的顺序从 1 连续编号。缺少任一源修订则整次拒绝。`read_release` 按固定发布及分区领取明细和统计。
- `open_job`、`handoff` 和 `issue_statement` 的 `bounds` 为闭区间 `[首行, 末行]`。交班目标作业可在转交时创建。已有的接班作业必须属于同一目标和发布。范围越界、存在冲突或部分越权时整次拒绝。已收行沿用实收回执，未决行保留原尝试占用，其他行可继续发送。
- 中心尝试号唯一。`send` 对同一作业、行和尝试号的重投返回本地已知结果。待查结局用 `reconcile` 查询。独立目标按目标和尝试号保留首次终态，同号异行拒绝。明确未收是目标的持久终态，可以挡住再到达的同号发送。尚未查到原号时仍保持未决。
- 分区账和清单账的 `row_count` 包含缺测，`missing_count` 只数缺测，`total` 从零开始累加非空读值。清单只核算所选区间。实收覆盖以目标、发布、冻结行号匹配，两个读数相同的原行各有独立出处及回执。
- `issue_statement` 在全部覆盖齐备后提交快照，`read_statement` 按原号获取原冻结顺序、读数、出处、回执及统计。源批新增修订及原回执重投不改变清单。

Python 可通过 `lake.audit.workday.initialize_workspace(path)` 获取工作区，再用 `with workspace.connect() as (center, receiver)` 打开连接。九个业务函数分别在 `lake.intake.batches`、`lake.release.freeze`、`lake.release.history`、`lake.dispatch.jobs`、`lake.dispatch.handoff`、`lake.delivery.attempts`、`lake.delivery.reconcile`、`lake.audit.statement` 和 `lake.audit.readback` 中定义。中心接口首参数为 `center`，`send` 与 `reconcile` 另接 `receiver` 为第二参数。API 返回数据类或不可变序列。业务异常属于 `lake.storage.LakeError`，CLI 将异常类别和消息写入标准错误并返回 2，存储或文件错误返回 3。

## 更正接续

已签普通清单固定了原行和实际回执。更正发布可能调换原行顺序、删行或换用另一来源，档案方需要一份新的有效口径，同时旧单仍按原号复核。接续的业务规则见 `CONTINUATIONS.md`。

```sh
python3 -B -m lake.audit.workday --workspace ./example-workday issue-continuation '{"target":"archive","request_id":"correction-1","parent_statement_id":1,"child_release_id":2,"partition":"east"}'
python3 -B -m lake.audit.workday --workspace ./example-workday active-partition '{"target":"archive","parent_statement_id":1}'
python3 -B -m lake.audit.workday --workspace ./example-workday read-continuation '{"continuation_id":"continuation-1"}'
```

- `issue_continuation(center, receiver, target, request_id, parent_statement_id, child_release_id, partition)` 先按父单和子发布核对。未齐返回 `{"status": "pending", "missing_lines": [...]}`，此时请求号未占用、父账仍有效；齐备时目标在一次本地提交中确认整份子账并消耗父单，中心再据该确认签结，返回 `{"status": "issued", "continuation_id": ...}`。
- `read_active_partition(receiver, target, parent_statement_id)` 只读目标库，返回 `uncontinued` 或该目标的首执。
- `read_continuation(center, continuation_id)` 只读中心库，按接续单号返回同一首执。

沿用判断只看冻结出处：站点、日期、修订、批内原位和读值均相同即沿用父回执，冻结行号移位不影响。其余子行必须由该目标实际收下该子发布的同一冻结行；同值异源不算。未沿用的父行按父单原序列为退出，保留原行和原回执，旧 V1 清单和实际流水不变。已确认的同号异参及另号接续已消费父单均拒绝；中心签结提交失败后重开两库，同号同申请取目标原确认补签，再投返回同一接续单号。
