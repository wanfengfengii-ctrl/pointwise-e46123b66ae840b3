# 已签分区的更正接续

已签的普通清单固定了某次分区交付的原行和实际回执。更正发布可能调换原行顺序，也可能删行或换用另一个来源。档案方要确认一份新的有效口径，接班员还要能取回旧单。普通签结、实际到件流水及旧单领取见 `README.md`。

## 申请与核对

- 父清单是该目标已签发的普通 V1 清单，明细必须恰好覆盖其发布中同名分区的全部冻结行。子发布是另一个已冻结发布，须包含该分区。范围以指定父单号为单位，每个目标的同一父单最多确认一次。接续单不能用作父单。
- 按子分区原序交付。仅当站点、日期、修订、批内原位和读值均相同，才沿用该父行的原目标回执，冻结行号变化不影响沿用。沿用项在子账显示子行，回执仍指向父发布中的原行。未沿用的父行按父单原序列为退出，保留其原行和原回执。空子分区可以确认，全部父行退出，子账统计均为零。
- 其余子行使用该目标实际收到的该子发布冻结行回执。目标根据自己保存的到件事实核对回执身份及完整冻结原行，别的目标、别的发布或别的原行的到件均不能代替。目标已实际收下但中心尚未查证的子行也算到件。多份实际回执如均与该子行完全匹配，可选其中一份并随首执固定。新收件沿用普通补送。接续只确认账面及其已有实收证据，历史流水和 V1 清单可照常领取。

## 确认与补签

目标在一次本地提交中确认整个有效子账及首执，同时消耗父单的一次接续资格。缺行时按子分区原序返回缺少的冻结行号，目标提交失败时也维持父账。两种情况下都无新确认或新清单，请求号未占用。

中心只凭目标成功确认的口径签结。若目标已确认但中心签结提交失败，目标立即认该子账。关闭并重新打开两库后，同号同申请取原确认并补签，完成后再投返回同一接续单号。申请身份为同一工作区内的 `request_id` 及其绑定的 `target`、`parent_statement_id`、`child_release_id`、`partition`。已确认的同号异参及另号接续已消费父单均拒绝，原确认和原清单保持。目标查询可单独使用目标库，中心按号领取可单独使用中心库。后续普通补送、源修订和重开不改变确认或接续单。

## Python 接口

连接仍使用 `lake.audit.workday.initialize_workspace(path)` 和 `workspace.connect()`。重复初始化支持已有 V1 工作区，并保留历史数据。`target`、`request_id` 和 `partition` 使用 V1 的非空标识规则。父清单号和子发布号使用 V1 正整数编号。下面的返回值均为可 JSON 序列化的字典。非法引用、非法输入或请求冲突以 `lake.storage.LakeError` 的子类报告，提交故障须报告失败。

- `lake.audit.statement.issue_continuation(center, receiver, target, request_id, parent_statement_id, child_release_id, partition)`。待齐返回 `{"status": "pending", "missing_lines": [行号, ...]}`。签结成功返回 `{"status": "issued", "continuation_id": 编号}`，编号为中心分配的非空字符串，供接续单领取使用。
- `lake.delivery.receiver.read_active_partition(receiver, target, parent_statement_id)`。目标尚未确认接续时返回 `{"status": "uncontinued"}`，确认后返回 `{"status": "confirmed", "confirmation": 首执}`。
- `lake.audit.readback.read_continuation(center, continuation_id)`。返回 `{"continuation_id": 编号, "confirmation": 首执}`。未知接续单以 `lake.storage.NotFound` 或其子类报告。

两处读到的首执具有相同内容。

| 字段 | 内容 |
| --- | --- |
| `confirmation_id` | 目标分配且在该目标库内唯一的非空字符串。重领保持原值 |
| `target`, `request_id`, `parent_statement_id`, `parent_release_id`, `child_release_id`, `partition` | 本次确认绑定 |
| `rows` | 按子分区原序排列。每项包含 `row`、`receipt` 和 `kind`。`kind` 为 `reused` 或 `received`，分别表示沿用父回执或使用子行新收件 |
| `retired_rows` | 未沿用的父行，按父单原序排列。每项包含原 `row` 和原 `receipt` |
| `balance` | 仅对 `rows` 核算，字段为 V1 的 `row_count`、`missing_count` 和 `total` |

`row` 和 `receipt` 使用 V1 `FrozenRow` 和 `Receipt` 的字段形状，即对应数据类的 `dataclasses.asdict` 结果。示例中移位的 A 的 `row.key.line` 为 2，`receipt.key.line` 为 1，二者发布号分别为子、父号。统计沿用 V1 整数精度及缺测口径。

## 样例及联合核验

`continuation_examples.py` 的 `prepare_example(center, receiver)` 在新工作区准备已签父账 A=5、B=11，固定子分区 C=11、A=5，同时准备只含原行的竞争子分区 B、A。`example.request()` 返回首个子分区的申请参数，可直接传入接续签结函数。`deliver_c(center, receiver, example)` 执行普通补送，可用 `target="other-archive"` 造另一目标实收，或用 `hide_reply=True` 模拟目标已收但中心未取到回包。`python3 -B continuation_examples.py` 运行 V1 对照，显示普通签结缺子第 2 行，补送两条子行后流水有四笔，两个普通清单总量均为 16。

1. C 未到或只送达 `other-archive` 时，接续返回 `pending` 和 `[1]`，目标仍为 `uncontinued`。给 `archive` 补送 C 后，即使隐藏回包，目标也能认出自己的实际到件。此时 `archive` 的实际流水是 `parent-A`、`parent-B`、`archive-C` 三笔。
2. 用夹具的 `deny_next_commit(db)` 对目标确认的提交注入一次 SQLite 拒绝，核对目标仍为 `uncontinued` 且无新签单。撤去注入，以同申请重试。接着覆盖目标确认成功而中心签结提交失败的边界，用同一夹具在中心签结提交处注入。上下文返回的查询函数可确认拒绝已触发。两处注入应分别落在各自的业务提交上。
3. 双库重开后，目标仍能独立取到原确认。另号申请竞争子分区须拒绝，已确认的原请求号改绑到竞争子分区也须拒绝。同号同参补签成功，再投返回同一接续单号。新单的首执等于目标原确认，原序为 C、A，回执依次为 `archive-C` 和 `parent-A`，退出 B 带 `parent-B`。新单仍是 2 行、0 缺测、合计 16。原流水仍为三笔，旧父单按原号领取与原件一致。

项目运行清单为 `.pointwise-runtime.json`，当前为 `version: 1`、`kind: cli`。保留 `setup` 的命令列表约定，`verify` 和 `demo` 各为一条 argv 命令。接通 `verify` 执行原回归及本功能联合核验，失败须返回非零。接通 `demo` 展示补送和双库重开补签后的目标有效账及旧单，原演示命令仍可独立运行。
