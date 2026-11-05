---
title: Judge HTTP API
---

# Judge HTTP API

Judge 服务器提供 REST API，用于提交代码归档、轮询评测结果、以及管理交互式游戏会话。通过 `sforge serve --port 8080` 启动。

## 基础 URL

```
http://localhost:8080
```

所有端点均以 `/api/v1` 为前缀。

## 认证机制

控制器 API 使用**基于 Token 的会话模型**。控制器注册会话后把 Judge
Token 保留在宿主机；work 容器只接收 runner 提供的、绑定当前注册的反馈
relay URL。relay 仅允许 Agent 提交、Agent 可见历史，以及读取同一注册已接受
的 Agent 提交结果。

## 接口列表

### 任务发现

列出从 `tasks/` 目录加载的所有可用任务。

```http
GET /api/v1/tasks
```

**响应：**

```json
[
  {"task_id": "ad_placement_optimization", "name": "Ad Placement Optimization"},
  {"task_id": "gitlet", "name": "Gitlet"},
  {"task_id": "tinykv", "name": "TinyKV"}
]
```

### 会话注册

注册新会话并获取 Token，用于后续提交。

```http
POST /api/v1/register
Content-Type: application/json

{
  "task_id": "ad_placement_optimization",
  "run_id": "run-001"
}
```

**响应：**

```json
{
  "token": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "feedback_relay_id": "11223344556677889900aabbccddeeff"
}
```

Token 是一个 32 字符的十六进制字符串，内部维护任务 ID、运行 ID 以及两个独立的计数器：Agent 提交（`agent-1`、`agent-2`、...）和自动评测提交（`auto-1`、`auto-2`、...）。

### 封存并接续原生评测注册（显式开启）

使用 `sforge serve --checkpoints --port 8080` 开启以下控制器接口；默认关闭时接口返回 404，也不创建 checkpoint 库。停用时去掉此开关重启，保留私有库及接续运行的占用标记供检查；独立实验使用新的 run ID。

所有 checkpoint 接口都要求 JSON 正文中的当前评测控制器 `admin_secret`。Agent 的提交 token 只能标识捕获来源，不能授权捕获或恢复。迁往另一控制器时，只把获授权的 `logs/checkpoints/` 库转入其自身日志目录，使用新控制器独立生成的管理密钥；不要复制原管理密钥文件或会话 token。

- `POST /api/v1/checkpoint/capture`：正文包含 `token`、`admin_secret`，可选 `capture_id`（调用者生成的 32 位十六进制操作 ID）。同一注册重试同一 ID 返回首次捕获的点，不采用后来的提交，也不重复保存；其他注册不能复用。省略该字段表示新的手动捕获。响应包含 `checkpoint_id`、`manifest_sha256`、题目/来源 run、已用轮次/剩余冷却，以及原生选优的 incumbent 轮次、大小和摘要。私有 capsule 保留已公开的 agent 历史、完整结果及被选中的归档，不包含提交或管理 token。归档按原始字节保留，不解包。
- `POST /api/v1/checkpoint/restore`：正文包含 `checkpoint_id`、捕获响应中的 `manifest_sha256`、全新的 `run_id` 和当前 `admin_secret`。响应返回新的提交 token 和反馈 relay。控制器通过 `/api/v1/history?token=...` 和 `/api/v1/result/{submission_id}?token=...` 读回。历史结果使用新的查询 ID，原报告保持不变；`logs/runs/新run/checkpoint-origin.json` 保存来源映射，incumbent 保留原轮次。

Capsule 的 manifest 始终使用严格 JSON。有限分数和指标继续使用
`sforge_judge_checkpoint_v1`。历史或报告中存在非有限数值时，使用
`sforge_judge_checkpoint_v2`：只将 `history`、`results` 中对应数值存为 `null`，
并在顶层 `nonfinite_values` 列表记录原值，例如
`{"path":["history",0,"score"],"kind":"positive_infinity"}`。
kind 仅支持 `positive_infinity`、`negative_infinity`、`nan`；path 的每一项必须
是确切的对象键或非负数组下标。这只是存储表示，不是缺失反馈。读取时先还原
原始数值，再核对历史、结果和原生选优。报告中的同名字符串、相似形状的字典
仍是普通数据。未知版本、重复或不存在的路径、非 null 目标、越过历史/结果
范围的路径及未标记的非有限数字均拒绝。旧 reader 会拒绝 v2，不会将占位 null
当成分数；新 reader 继续支持有限 v1 格式，但两种格式均保留原有精确源码/
评测器绑定要求。

恢复的 `report.json` 沿用正常评测生成的原生报告格式，保留 Python JSON 的
`Infinity`、`-Infinity`、`NaN` 表示，不把 capsule 的路径编码带入普通报告。
运行中的报告、选优和公开 API 投影保持原有语义；捕获和恢复不会把非有限分数
改成零或缺失反馈。

恢复不会评测或多消费一轮。后续提交仍由原生 `consume_round`、预算、冷却及选优规则决定。冷却在新注册发布时，从捕获时剩余的**时长**重新计时；离线等待和 capsule 物化既不会补发机会，也不会消耗这段时长；旧 capsule 不包含来源后来取得的提升。

若恢复响应丢失，用**相同**的正文（`checkpoint_id`、`manifest_sha256`、`run_id` 及当前 `admin_secret`）调用 `POST /api/v1/checkpoint/restore-result`。响应结构与首次恢复相同，返回已发布的原提交 token；不创建注册、不重放评测、不重置冷却。后续提交发生后仍可读回，已用机会保持不变。来源标记必须匹配且原注册仍存活；缺失、错配、部分激活或已丢失的注册返回 409，管理授权错误返回 403，非法正文返回 422。评测进程重启后，仅凭磁盘来源标记不能重新生成凭证。此操作不恢复 runner 的阶段截止时间：控制器必须保存首次激活时的原截止时间，重试不能重新计一段预算。更换 `run_id` 是另一个分支，不是恢复原响应。

当前版本支持配置的 Docker backend 上已结算的非 game 注册，不支持自动评测历史。绑定原生 Python 源码、完整题目规格、实际 Docker 评测镜像 ID、选优策略/方向及有效 CPU/内存限制。字节缺失或变化、来源/评测不符、未完成结果、已消费却无公开历史的轮次、结果/历史冲突及不支持的配置返回 409；管理授权错误返回 403，非法正文返回 422。已有 run、重复恢复和中断留下的目标占用会拒绝，不会重置预算。完成前的异常回滚可重试；进程崩溃遗留的占用需要控制器检查。开启期间，已恢复 run ID 也不能再次冷注册，且整个 run ID 均被占用。

库是受信任的控制器私有状态；SHA-256 只能核对捕获引用，不能防止恶意管理员修改，也不能证明任意报告不含敏感内容。此接口不是完整 worker checkpoint：除 incumbent 外的工作代码、模型会话、研究状态、外部副作用权限及 runner 剩余总预算须由其各自 owner 保留，之后才能判断分支是否可比。停用后不要把已恢复 run 重新冷注册。

另有两个默认路径一致性修复：backend 准备失败时不发布 token/资源行；评测失败的历史改为 `status="error"`，与 `/result` 一致。成功冷注册的默认值、轮次分配及评分/选优规则不变。

### 提交归档

提交代码归档进行评测。服务器从 Token 中解析任务 ID 和运行 ID，并分配递增的轮次 ID。

```http
POST /api/v1/submit
Content-Type: multipart/form-data

token: a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4
archive: @solution.tar.gz
kind: agent
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `token` | string | 是 | 从 `/register` 获取的会话 Token |
| `archive` | file | 是 | `.tar.gz` 格式的解决方案归档 |
| `kind` | string | 否 | `"agent"`（默认）或 `"auto"`。决定递增哪个轮次计数器。 |

**响应：**

```json
{
  "submission_id": "a1b2c3d4e5f6",
  "round_id": "agent-1",
  "status": "queued"
}
```

提交异步处理——服务器立即返回 `status: "queued"`。通过 result 接口轮询结果。

### 可选来源回执与 JSON 客户端输出

`sforge-submit --json` 在 stdout 输出一份原生 JSON 响应，进度和错误写入
stderr。加 `--no-wait` 时只返回已受理提交，不读取评分；
`--result ID --no-wait --json` 只读一次同一提交，普通
`--result ID --json` 等待完成。JSON 保留完整公开报告，包括 details、零分、
负分、缺失分数及 `valid=false`。未完成时 report 为空；评分错误保留 error，
退出码非零。JSON 模式不读取或更新供人查看的本地缓存；省略 `--json` 保持
原有文本及缓存行为。`--list --json` 仅返回当前 relay 可见的历史和准入快照。

客户端在 submit 表单或 result 查询中传入 `include_receipt=true`。result
回执通过不透明、绑定 run 的反馈 relay 查询，work 容器既拿不到 judge URL，也
拿不到 judge token；直接调用宿主 API 查询回执时仍需 session token。两条链路
都只能读取当前注册可见的 agent 历史，不公开其他运行或宿主自动评测。未开启
时 API 响应字段保持原样。开启后，两种响应均包含：

```json
{
  "receipt": {
    "schema_version": "sforge_submission_receipt_v1",
    "task_id": "example",
    "round_id": "agent-1",
    "archive_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
    "archive_bytes": 1234
  }
}
```

摘要由 judge 在异步评分前对实际受理的归档字节计算；工作区修改、删除或响应
丢失后的读回仍关联外层同一 `submission_id`，不会重新提交、补发预算或重置
冷却。checkpoint 捕获和恢复会保留每个回执，并重映射查询 ID。旧结果缺少来源
时，归档字段明确为 null。旧服务器不支持回执时，无法提供 `--json` 来源回执；
应使用原文本客户端恢复，不能因此重提。上面的摘要只是示例。归档摘要不等于
源码树摘要、题目输入版本或评测器身份；比较仍须由 runner 独立验证这些条件。
回执匹配不证明实验可比、分数有效或候选最优。

### 获取结果

轮询提交的评测结果。

```http
GET /api/v1/result/{submission_id}?token={host_session_token}
```

**响应（排队中/运行中）：**

```json
{
  "submission_id": "a1b2c3d4e5f6",
  "status": "running",
  "report": null,
  "error": null
}
```

**响应（已完成）：**

```json
{
  "submission_id": "a1b2c3d4e5f6",
  "status": "completed",
  "report": {
    "total_tests": 50,
    "passed": 45,
    "failed": 5,
    "errors": 0,
    "pass_rate": 0.9,
    "score": null,
    "valid": true,
    "summary": "45/50 tests passed. Failed: test_forward, test_backward, ...",
    "test_details": [...]
  },
  "error": null
}
```

**响应（错误）：**

```json
{
  "submission_id": "a1b2c3d4e5f6",
  "status": "error",
  "report": null,
  "error": "Container exited with code 137 (OOM killed)"
}
```

**状态值：**

| 状态 | 说明 |
|------|------|
| `queued` | 已接收提交，等待处理 |
| `running` | Judge 容器正在运行测试 |
| `completed` | 评测完成，报告可用 |
| `error` | 评测失败（容器崩溃、超时等） |

### Agent 反馈 Relay

runner 在 work 容器内把 `SFORGE_FEEDBACK_URL` 设置为
`/api/v1/feedback/{feedback_relay_id}`。生成的 `sforge-submit` 只调用三个无需
Judge 凭据、且绑定当前注册的接口：

```http
POST {SFORGE_FEEDBACK_URL}/submit
GET  {SFORGE_FEEDBACK_URL}/history
GET  {SFORGE_FEEDBACK_URL}/result/{submission_id}
```

relay 固定以 `agent` 身份提交，隐藏 auto-eval 历史，并拒绝其他 run 或宿主机
auto-eval 的结果 ID。原始控制器 result 接口仍要求宿主机持有的会话 token。

### 提交快照与读取结果

Judge 评测提交时上传的归档。提交被接受后，继续修改工作目录不会改变这次提交。
如果希望提交后继续独立工作，等需要反馈时再读取结果：

```bash
sforge-submit --no-wait
# 保留输出的提交 ID 及其对应的候选版本，继续工作。
sforge-submit --result <submission_id> --no-wait --details
```

新提交使用 `--no-wait` 时，在提交被接受后成功退出，此时尚未读取或缓存分数。
与 `--result ID` 同用时，先核验 relay 可见历史，再查询一次结果：`queued` 或
`running` 会成功返回明确的 pending 状态，不输出分数、不修改缓存；终态结果沿用
阻塞查询的反馈与错误处理。pending 的成功退出只表示查询成功，不表示候选通过验收。
可以先继续有用的独立工作，再查询同一 ID。

不加 `--no-wait` 时，提交与结果查询仍等待评测反馈。`--no-wait` 不能与 `--list`
同用；与 `--details` 同用时必须提供 `--result ID`。需要恢复的候选版本应自行保留；
结果查询只读取反馈，不恢复源码。定期自动评测会独立捕获之后的工作目录状态。

生成的 `sforge-submit` 工具可以读取已接受提交的结果，无需再次评测：

```bash
sforge-submit --list
sforge-submit --result <submission_id> --details
```

使用接受提交时输出的 ID；若接受响应丢失，先通过 `--list` 查找。
恢复读取仅接受当前 relay 可见历史中的 ID，保留原轮次、分数和公开详情，
不打包当前工作目录、不消耗新的提交次数，也不改变冷却时钟。
单独使用 `--details` 仍会提交新代码；读取原详情应使用 `--result ID --details`。

网络或 HTTP 错误、评测终态错误，以及完成但缺少报告的响应，都会以非零退出码
报错，不写入零分缓存。真实零分仍是零分，缺失分数仍保持缺失。
接受状态不确定时，先检查历史再决定是否重新提交。此功能是客户端结果读取，
不提供提交重试的幂等性或服务端重启后的持久恢复；会话和结果必须仍保留在服务端。

### 运行历史

获取某次运行会话的完整提交历史，包括最佳分数的选定结果。

**通过 Token 查询：**

```http
GET /api/v1/history?token=<token>
```

**通过运行 ID 查询：**

```http
GET /api/v1/runs/{run_id}/history?task_id=<task_id>
```

**响应：**

```json
{
  "run_id": "run-001",
  "best_pass_rate": 0.95,
  "best_score": null,
  "best_round": "agent-3",
  "agent_submissions": 4,
  "auto_submissions": 2,
  "entries": [
    {
      "type": "submission",
      "status": "completed",
      "submission_id": "abc123",
      "task_id": "ad_placement_optimization",
      "round": "agent-1",
      "pass_rate": 0.8,
      "score": null,
      "passed": 40,
      "failed": 10,
      "total_tests": 50,
      "valid": true,
      "summary": "40/50 tests passed. Failed: ..."
    }
  ]
}
```

`best_*` 字段根据任务配置的选择策略计算（参见[Benchmark 与任务接入](/zh/tasks/integration-guide#选择策略)）。

### 游戏接口

对于 `game_mode: true` 的任务，Judge 服务器在专用容器中管理交互式游戏会话。

#### 创建新游戏

```http
POST /api/v1/game/{run_id}/{task_id}/new
Content-Type: application/json
{}
```

**响应：**

```json
{
  "session_id": "abc123def456",
  "observation": "You are standing in a dark room...",
  "score": 0,
  "peak_score": 0,
  "max_score": 100,
  "done": false,
  "moves": 0
}
```

#### 执行动作

```http
POST /api/v1/game/{run_id}/{task_id}/{session_id}/step
Content-Type: application/json

{"action": "go north"}
```

**响应：**

```json
{
  "session_id": "abc123def456",
  "observation": "You enter a dimly lit hallway...",
  "score": 10,
  "peak_score": 10,
  "max_score": 100,
  "done": false,
  "moves": 1
}
```

#### 查询状态

```http
GET /api/v1/game/{run_id}/{task_id}/{session_id}/status
```

**响应：**

```json
{
  "session_id": "abc123def456",
  "score": 10,
  "peak_score": 10,
  "max_score": 100,
  "done": false,
  "moves": 1
}
```

#### 关闭会话

```http
POST /api/v1/game/{run_id}/{task_id}/{session_id}/close
```

**响应：**

```json
{
  "session_id": "abc123def456",
  "final_score": 25,
  "peak_score": 30,
  "max_score": 100,
  "moves": 15
}
```

#### 关闭所有会话

关闭指定运行和任务的所有活跃游戏会话。

```http
POST /api/v1/game/{run_id}/{task_id}/close-all
```

**响应：**

```json
{"closed": 3}
```

游戏会话有 10 分钟的空闲超时。当游戏结束（`done: true`）或被显式关闭时，会话会自动归档。系统最多支持 200 个并发游戏会话。

## curl 示例

### 注册、提交和轮询

```bash
# 1. 注册会话
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/register \
  -H "Content-Type: application/json" \
  -d '{"task_id": "ad_placement_optimization", "run_id": "run-001"}' \
  | jq -r '.token')

echo "Token: $TOKEN"

# 2. 提交归档
SUBMISSION_ID=$(curl -s -X POST http://localhost:8080/api/v1/submit \
  -F "token=$TOKEN" \
  -F "archive=@solution.tar.gz" \
  -F "kind=agent" \
  | jq -r '.submission_id')

echo "Submission: $SUBMISSION_ID"

# 3. 轮询结果（重复直到 status 为 "completed" 或 "error"）
curl -s "http://localhost:8080/api/v1/result/$SUBMISSION_ID?token=$TOKEN" | jq .

# 4. 查看运行历史
curl -s "http://localhost:8080/api/v1/history?token=$TOKEN" | jq .
```

### 轮询循环

```bash
while true; do
  STATUS=$(curl -s "http://localhost:8080/api/v1/result/$SUBMISSION_ID?token=$TOKEN" | jq -r '.status')
  echo "Status: $STATUS"
  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "error" ]; then
    curl -s "http://localhost:8080/api/v1/result/$SUBMISSION_ID?token=$TOKEN" | jq .
    break
  fi
  sleep 5
done
```
