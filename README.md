# 内网门户公告 · 缓存代理 + 双人审批模式切换

FastAPI + httpx 的单进程 HTTP 缓存代理，以及公告 public/personal 模式的
**双人审批**（四眼原则）变更通道。代理只服务 `GET /notices/{id}` 一个业务
接口，上游固定为随环境启动的本地上游服务；模式切换必须经一名运维发起、
另一运维复核，全链路 SQLite 持久化，超时/断流一律"待核实"。

## 目录结构

```
.
├── proxy/
│   ├── app.py             # 代理主应用：缓存路由、审批 API、Basic 认证、透传
│   ├── approval_store.py  # 提案 SQLite 状态机（原子迁移、提交闸、重启恢复）
│   ├── cachestore.py      # 内存缓存 + 同键在途合并 + 代际（epoch）清除
│   ├── policy.py          # Cache-Control 解析与缓存资格判定
│   └── static/
│       ├── index.html     # 缓存代理演示页（浏览器入口 /）
│       └── approval.html  # 双人审批页（/approval，HTTP Basic）
├── upstream/
│   ├── app.py             # 本地上游：公告、审批应用入口、丢回执开关、审计
│   └── mode_store.py      # 受控配置 + 回执 SQLite（去重/修订核对/同事务）
├── scripts/
│   ├── run-local.sh       # 无 Docker 时本地启动（uvicorn 双进程）
│   ├── stop-local.sh
│   ├── verify.py          # 端到端行为验证（87 项断言，仅用 stdlib）
│   └── test_units.py      # 单元测试（策略/缓存/两个存储层，31 项）
├── Dockerfile             # 代理与上游共用同一镜像
├── docker-compose.yml     # 一键环境（代理 :8000；上游不暴露宿主端口）
└── requirements.txt
```

## 快速启动

### 方式一：Docker Compose（推荐交付形态）

```bash
docker compose up --build
# 缓存代理演示页：http://localhost:8000/
# 双人审批页：    http://localhost:8000/approval
```

- `proxy`：唯一对外服务，端口 `8000`；上游固定 `http://upstream:8001`；
- `upstream`：模拟上游，**不暴露任何宿主端口**，仅 compose 内部网络可达；
- 两个运维账号由启动配置固定（`OPS_ACCOUNTS`，默认
  `alice:alice-pass,bob:bob-pass`，compose 中显式配置，可改）；
- 两侧 SQLite 分别落在命名卷 `proxy-data` / `upstream-data`，
  `docker compose restart` 后提案、配置、回执均可核实/重试。

### 方式二：本地直接运行（无 Docker）

```bash
pip install -r requirements.txt
scripts/run-local.sh            # 默认 :8000 代理 / :8001 上游（仅回环）
# 浏览器入口：http://127.0.0.1:8000/  与  http://127.0.0.1:8000/approval
scripts/stop-local.sh           # 停止
```

本地态：提案库 `.run/proxy-approval.db`，上游库 `.run/upstream.db`，
删除 `.run/` 即恢复出厂。

### 验证

```bash
# 单元测试（无需启动服务）
python3 -I scripts/test_units.py

# 端到端（先启动服务；compose 下上游不暴露宿主端口，脚本只访问代理）
PROXY_ADDR=127.0.0.1:8000 python3 -I scripts/verify.py
# 账号可用 VERIFY_OPS_A / VERIFY_OPS_B 覆盖（user:pass）
```

## 双人审批：规则与实现

| 需求 | 实现 |
| --- | --- |
| 双人审批页、固定两账号、HTTP Basic | `proxy/static/approval.html`；`proxy/app.py` `_ops_user`，账号由 `OPS_ACCOUNTS` 启动配置固定且必须恰好两个，否则进程拒绝启动 |
| 提案含公告 ID、目标模式、所见修订 | `ApprovalStore.create`；修订取自上游 `GET /admin/config-state` 当前 `revision` |
| 提交后不可修改 | 存储层不提供任何内容修改方法，只有状态迁移；`PUT/PATCH` 无路由（405） |
| 发起者不能批准 | `approve` 校验 `approver != proposer`（403）；上游 `/admin/apply` 二次校验 |
| 并发批准只生效一次 | 原子 `UPDATE ... WHERE status='PENDING'`，rowcount 定胜负（单事务） |
| 上游按提案 ID 去重 | `receipts.proposal_id` 主键；命中即返回原回执，`deduped=true` |
| 核对修订、旧修订拒绝 | `ModeStore.apply` 比对 `base_revision` 与当前 `revision`；拒绝也落成回执（409） |
| 配置与回执一起持久保存 | 同一 `BEGIN IMMEDIATE` 事务内改配置 + 修订 +1 + 写回执 |
| 同提案重试返回原回执、不重复生效 | 去重命中直接返回已存回执；修订不再前进 |
| 代理一次只提交一笔 | 提交闸：存在 `SUBMITTING/UNKNOWN` 提案时 `begin_submit` 拒绝（409），闸检查与迁移同事务 |
| 先保存批准与提交状态再请求上游 | 批准即落库；`begin_submit` 提交 `SUBMITTING` 后才发起 HTTP |
| 超时/断流显示待核实 | `httpx` 异常 → `UNKNOWN`（HTTP 202），不报成功/失败；页面红色"待核实" |
| 待核实不放行下一笔 | `UNKNOWN` 占用提交闸，须"核实"或"重试"该笔 |
| 双方 SQLite 持久化、重启可核实/重试 | 代理 `PROXY_APPROVAL_DB`、上游 `UPSTREAM_DB`；代理启动时 `SUBMITTING→UNKNOWN` 恢复 |
| 网络等待不持数据库写锁 | 所有迁移为独立短事务（WAL），HTTP 等待发生在两次事务之间；验证见 `verify.py` 第 16 节 |
| 原配置/重置入口不得绕过审批 | 上游 `/admin/config` 拒绝 `profile`（400）；`/admin/reset` 只重置演示旋钮，模式配置保持不变 |
| 上游不暴露宿主端口 | compose 中 `upstream` 无 `ports`，仅内部网络；主机侧一切经代理透传 |
| 页面可发起/批准/核实/查看实际配置 | 审批页 ②③⑤ 区；实际配置来自上游 `/admin/config-state` |
| 上游生效后丢回执开关 | `POST /admin/chaos`（页面④区，经 `/proxy/upstream/chaos` 透传）：开启后上游生效并持久化，但沉默 `DROP_RECEIPT_SECONDS`（默认 30s）不回执 |

### 提案状态机

```
PENDING --批准(另一账号)--> APPROVED --提交--> SUBMITTING
SUBMITTING --上游明确回执--> APPLIED(已生效) / REJECTED(已拒绝，如旧修订)
SUBMITTING --超时/断流-----> UNKNOWN(待核实：既非成功也非失败)
UNKNOWN --核实(上游回执)--> APPLIED / REJECTED
UNKNOWN --重试(同提案ID)--> SUBMITTING        （上游幂等，不重复生效）
进程重启：SUBMITTING 一律恢复为 UNKNOWN（结果不可知，须核实）
```

### 关键环境变量

| 变量 | 服务 | 默认 | 说明 |
| --- | --- | --- | --- |
| `OPS_ACCOUNTS` | proxy | `alice:alice-pass,bob:bob-pass` | 固定两个运维账号（`user:pass,user:pass`） |
| `APPROVAL_SUBMIT_TIMEOUT_SECONDS` | proxy | `5` | 提交上游的超时；超时即待核实 |
| `PROXY_APPROVAL_DB` | proxy | `.run/proxy-approval.db`（compose：`/data/`） | 提案 SQLite |
| `UPSTREAM_DB` | upstream | `.run/upstream.db`（compose：`/data/`） | 受控配置+回执 SQLite |
| `DROP_RECEIPT_SECONDS` | upstream | `30` | 丢回执时上游沉默时长（须大于提交超时） |
| `UPSTREAM_BASE` / `UPSTREAM_TIMEOUT_SECONDS` | proxy | `http://upstream:8001` / `15` | 业务转发 |

## 审批页操作指引（http://localhost:8000/approval）

1. 浏览器弹出 Basic 认证，用任一运维账号登录（另一账号用于复核）；
2. **② 上游实际配置**：查看当前修订、默认模式、按公告覆盖（权威实况）；
3. **③ 发起提案**：选公告与目标模式，"所见修订"自动取当前值；创建后不可修改；
4. **⑤ 提案列表**：另一账号"批准"→ 任一账号"提交"→ 观察 已生效/已拒绝；
5. **④ 丢回执开关**：开启后提交将演示"生效但回执丢失"——提案变红
   **待核实**，提交闸关闭；用"核实"从上游取回真实回执（或"重试"原提案），
   闸随即释放，下一笔才可提交；
6. 可配合 `docker compose restart proxy`（或本地杀掉代理进程）验证：
   重启后提交中的提案变为待核实，可继续核实/重试，上游配置与回执不丢。

## 缓存代理行为规则（既有，保持不变）

| 需求 | 实现 |
| --- | --- |
| 仅代理 `GET /notices/{id}`，上游固定 | `proxy/app.py` 单一路由；`UPSTREAM_BASE` 环境变量；其他路径 404、非 GET 405 |
| 缓存按公告 ID + Accept-Language 区分 | `CacheStore.make_key`，语言取请求头原始值，缺失记为 `<none>` |
| 带 Cookie / Authorization 直连，不读写缓存、不参与合并 | `_is_personal` 判定，响应头 `X-Cache: BYPASS` |
| 仅 200 可缓存；须含 `public` 与合法非负整数 `max-age`；拒绝 `private`/`no-store`/`Set-Cookie` | `policy.cache_eligibility` |
| 过期不复用 | `CachedResponse.expires_at` 单调时钟判定，命中过期即剔除 |
| 同键并发未命中只请求上游一次 | `CacheStore.lookup` leader/wait 合并，等待同一 `asyncio.Future` |
| 一个等待者断开不影响其他人 | 上游请求在独立 `asyncio.Task` 中执行 |
| 失败不缓存且结束所有等待 | `fail_all`：传输异常与 5xx 均 502 `X-Cache: ERROR` |
| 按公告清除全部语言缓存；旧在途不回填、新请求不复用旧在途 | 代际（epoch）+ `purged` 标记，`store_if_current` 拒绝过期代际 |

### 响应头观测口径

- `X-Cache`：`HIT` / `MISS` / `MISS-NO-CACHE` / `MISS-WAIT` / `BYPASS` / `ERROR`；
- `X-Cache-Reason`：不可缓存或绕过的具体原因；
- `X-Cache-Key`、`Age`：仅 HIT 时返回；
- `X-Upstream-Request-Id`：透传上游请求 ID，同一次合并的等待者该值相同。

## 设计说明与边界

- 缓存为单进程内存态，重启即清空（运维缓存定位）；审批提案、受控配置与
  回执为 SQLite 持久态，重启保留；
- 上游 5xx 按失败处理；4xx（如 404）为成功送达但非 200 不可缓存；
- 提交闸是进程内 + SQLite 事务双重保证；多副本部署需额外领导者选举，
  本环境为单代理进程，符合交付定位；
- 演示管理接口（`/proxy/*`、上游 `/admin/*`、混沌开关）仅用于内网演示，
  不应暴露到公网；审批接口（`/approval/*`）强制 HTTP Basic；
- 演示旋钮（延迟/失败/max-age/自定义 Cache-Control）非受控配置，用于
  缓存行为演练，不纳入审批范围。
