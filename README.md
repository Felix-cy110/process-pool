# Rust 可复用进程池

这是一个受 Java `ThreadPoolExecutor` 启发的 Rust 进程池。调用方传入 7 个参数初始化，worker 默认按任务需要创建，也可以显式预热。通过 stdin/stdout 上的 NDJSON 协议反复向同一子进程发送任务，从而复用进程启动、运行时加载和初始化成本；外部调用方通过 HTTP JSON-RPC 提交 JSON 任务。

当前版本是可运行的单机进程池，不是分布式任务队列。它适合 Python/Node/JVM 模型推理、编译器、脚本运行时等“启动贵、单次调用相对短、进程可重复接收请求”的 worker。

## 为什么选 JSON-RPC

调用分成两层：

```text
任意语言调用方
      │ HTTP + JSON-RPC 2.0
      ▼
process-pool-server（排队、扩缩容、超时、拒绝策略）
      │ stdin/stdout + NDJSON，每个 worker 同时只执行一个任务
      ▼
长期存活并可复用的 worker 子进程
```

- HTTP JSON-RPC 比自定义 TCP 协议更容易被任意语言调用，也比一开始引入 gRPC/Protobuf 更适合当前“任意 JSON 任务”的需求。
- 内部使用 stdio，worker 不需要占用端口，也不需要自己做服务发现。
- 如果未来任务已经形成强类型 schema，并需要流式返回、双向流或极高吞吐，可以在进程池上层增加 gRPC，底层池不需要重写。

RPC 提供初始化、执行任务、查询状态和显式预热。7 个参数由调用方提供，但 `process_factory` 只能传服务端预先登记的名称（如 `echo`），不能传任意程序、命令参数或环境变量。名称到实际 worker 配置的映射由本地 [`examples/worker-factories.json`](examples/worker-factories.json) 管理；这是本地白名单，不是分布式注册中心。

服务默认只监听本机且没有鉴权，初始化和任务执行接口都应视作管理接口。进程工厂白名单不等于鉴权，也不能防止不安全 worker 自身执行危险任务；对外开放前必须增加访问控制和资源限制。

## 7 个初始化参数

[`examples/initialize-rpc.json`](examples/initialize-rpc.json) 的 `params` 严格包含下面 7 个参数，全部必填，没有隐式默认值。示例文件和 Web 表单中的数字只是可修改的示例，不会自动生效。

| 参数 | Java 对应概念 | 本项目语义 |
| --- | --- | --- |
| `core_pool_size` | `corePoolSize` | 按任务到达逐步创建并保留的核心进程目标数，可以为 0；初始化不启动进程 |
| `maximum_pool_size` | `maximumPoolSize` | 池内受管理进程的最大数量，必须大于 0 |
| `keep_alive_time` | `keepAliveTime` | 非核心空闲进程的存活数值 |
| `time_unit` | `TimeUnit` | `milliseconds`、`seconds` 或 `minutes` |
| `work_queue` | `BlockingQueue` | 当前实现为有界队列；容量 0 等价于直接移交任务 |
| `process_factory` | `ThreadFactory` | RPC 传已注册工厂名称；Rust 库和受信任本地配置传 worker 程序、参数、环境变量覆盖和工作目录 |
| `rejected_execution_handler` | `RejectedExecutionHandler` | `abort`、`discard`、`discard_oldest` 或 `caller_runs` |

调度顺序参考 Java 线程池：

1. 少于核心数时，随任务到达创建 worker，即使已有空闲 worker。
2. 达到核心数后，优先复用空闲 worker。
3. 没有空闲 worker 时先进入队列；核心数为 0 且当前没有 worker 时，至少创建一个来执行任务。
4. 队列满后再扩容，直到最大进程数。
5. 最大进程数也已忙时执行拒绝策略。

对 RPC 服务建议使用 `abort`，调用方会立即收到 `-32001` 并自行退避。`caller_runs` 在进程语义下会启动一个不计入 `maximum_pool_size` 的一次性进程，只是对 Java 策略的近似模拟，持续过载时可能产生大量进程。

## 快速开始

需要支持 Rust 2024 edition 的工具链。

```bash
cd /Users/chenyang/process-pool
cargo build --bins
cargo run --bin process-pool-server
```

默认只监听 `127.0.0.1:7788`。此时只是 HTTP 服务启动，进程池尚未初始化，worker 数量为 0。服务主进程本身仍会占用系统资源。

如果你还在运行旧版本，请先在原终端按 Ctrl+C，再执行上面的命令。不要带 `--config`，即可体验 Web/RPC 初始化流程。

### Web 监控面板

服务启动后在浏览器打开：

```text
http://127.0.0.1:7788/
```

未初始化时，页面展示 7 个参数的编辑表单。点击初始化后进入运行监控；此时 worker 仍为 0。提交任务后按需创建，或点击“预热核心进程”一次性补齐到核心数。

监控面板每秒读取一次本机进程池状态，展示：

- 当前、核心和最大 worker 数量，以及忙闲比例；
- 等待队列、成功、失败、拒绝和 Caller Runs 任务数；
- 最近一分钟的忙碌进程和排队任务趋势；
- 每个 worker 的 PID、状态、当前任务、运行时间和最近执行耗时；
- 核心运行参数、采集延迟和当前浏览器会话内的监控事件。

原始监控数据也可以通过 `GET /api/stats` 获取；已注册的工厂名称可通过 `GET /api/factories` 获取。Web 页面与 RPC 共用一个服务端口，worker 仍然只使用 stdin/stdout，不占用额外端口。页面资源随 Rust 二进制内嵌，不需要另起前端服务。

### 在 Web 中直接调试

页面的“接口调试”区直接调用同源 `POST /rpc`，不是模拟器，不需要另开终端：

1. 通过上方七参数表单初始化，或者在调试区选择 `pool.initialize`。初始化模板会读取上方表单的当前值；JSON 编辑框只填写 `params` 对象，不需要自己填写 `jsonrpc`、`id`、`method`。
2. 选择 `pool.execute` 投放任务，可以载入求和、回显、耗时任务或业务错误示例，修改 JSON 后发送。示例适用于内置 `echo` worker，自定义 worker 要改成自己的 `payload`。
3. 查看完整请求、完整响应、HTTP 状态、RPC 错误、耗时和返回的 worker PID。调用记录保留最近 30 条，仅存在当前页面内存中，刷新页面即清空；上方表单初始化和预热按钮的调用也会记录。
4. 如需提前创建进程，选择 `pool.prestart`，参数填 `{}`；查询状态选择 `pool.stats`，同样填 `{}`。

“同时投放数量”表示发送多个独立的真实任务请求，可设为 1–16；初始化、预热和状态查询每次只调用一次。16 是 Web 调试器的单次投放保护上限，不是进程池的最大进程配置。请求不会自动重试，点击记录也不会重新投放任务。

想观察排队和扩容，可以在一个尚未初始化的测试实例中设置：核心数 `1`、最大数 `3`、队列容量 `1`、拒绝策略 `abort`；选择耗时任务，并发投放 `5` 个。通常可看到 3 个忙碌 worker、1 个排队任务，以及 1 条 `-32001` 拒绝响应。核心数设为 `1` 时，串行调用回显可观察相同 PID 被复用。

JSON 参数编辑框限制为 64 KiB。`timeout_ms` 是 worker 的任务执行超时，不含排队等待；Web 的“HTTP 等待上限”控制整个请求的等待时间（含排队，默认 60 秒，最多 300 秒）。HTTP 超时或断网只能说明结果未确认，不代表服务端任务已取消；请先查询池状态，不要直接重复提交有副作用的任务。

调试功能会真实改变当前实例的进程和任务状态，不要对正在处理重要业务的池随意批量投放。工厂仍然只能选择服务端登记的名称，不能通过 Web 输入任意系统命令。当前不提供运行中热修改参数、任意新增单个 worker 或强制终止指定进程的管理接口。

### RPC 调用

另开一个终端，先初始化（如果已经通过 Web 初始化，跳过此步）：

```bash
curl -sS http://127.0.0.1:7788/rpc \
  -H 'content-type: application/json' \
  --data-binary @/Users/chenyang/process-pool/examples/initialize-rpc.json
```

你可以修改这个请求文件中的全部 7 个参数。初始化响应中 `initialized` 为 `true`，`worker_count` 为 `0`。未初始化就提交任务或预热，会返回 `-32006`。

然后提交任务：

```bash
curl -sS http://127.0.0.1:7788/rpc \
  -H 'content-type: application/json' \
  -d '{
    "jsonrpc":"2.0",
    "id":1,
    "method":"pool.execute",
    "params":{"payload":{"op":"sum","values":[7,11,24]},"timeout_ms":5000}
  }'
```

响应示例：

```json
{"jsonrpc":"2.0","id":1,"result":{"pid":12345,"sum":42.0}}
```

示例核心数为 2，因此前两个任务会分别创建 worker；达到核心数后复用已有空闲进程。要简单验证连续调用返回同一个 PID，可以初始化时把核心数设为 1，再串行提交任务。

可选：不等任务到达，显式预热核心进程：

```bash
curl -sS http://127.0.0.1:7788/rpc \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":3,"method":"pool.prestart","params":{}}'
```

返回的 `started_worker_count` 是本次新建数，只补齐缺少的核心进程；重复调用不会再创建多余进程。

查询池状态：

```bash
curl -sS http://127.0.0.1:7788/rpc \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"pool.stats","params":{}}'
```

健康与就绪接口：

```bash
curl -sS http://127.0.0.1:7788/healthz
curl -sS http://127.0.0.1:7788/readyz
```

- `/healthz` 表示 HTTP 服务存活，未初始化也返回 200。
- `/readyz` 在未初始化时返回 503；初始化后返回 200，即使还没有 worker。它不验证 worker 的业务可用性。
- `/api/stats` 和 `pool.stats` 在未初始化时返回 `{"initialized":false}`，初始化后返回 `initialized: true` 和完整指标。

### 配置生命周期

一个服务实例目前只承载一个池。`pool.initialize` 只允许成功一次；并发初始化也只会有一个成功，重复调用返回 `-32007`，不会替换正在运行的池或丢弃任务。参数可在每次初始化时自由配置，但本版本不提供运行中热修改。要换配置需重启，RPC 初始化结果不写回磁盘。

如需启动时自动从受信任的本地文件初始化，仍支持：

```bash
cargo run --bin process-pool-server -- --config examples/pool-config.json
```

这只跳过 Web/RPC 初始化步骤，仍不会自动预热。该本地文件的 `process_factory` 是完整 worker 配置对象，而不是 RPC 中的注册名称，因此本地配置模式不要求额外提供工厂白名单。RPC 模式默认读取 `examples/worker-factories.json`，也可以通过 `--factories /absolute/path/worker-factories.json` 指定；其中的相对可执行路径按服务工作目录解析。

Rust 项目也可直接用 `PoolConfig::new(七个参数)` 和 `ProcessPool::new(config).await` 创建池，无需 HTTP；按需调用 `pool.prestart_core_workers().await` 预热，再调用 `pool.execute(payload, timeout).await` 提交任务。

## 接入自己的 worker

worker 是任意可执行程序，只需：

1. 保持进程存活，逐行读取 stdin。
2. 每行解析一个 [`WorkerRequest`](docs/worker-protocol.md)。
3. 完成任务后向 stdout 写一行对应的 `WorkerResponse` 并 flush。
4. 不要向 stdout 写日志；日志写到 stderr。

项目内的 [`echo-worker`](src/bin/echo-worker.rs) 是完整参考实现。协议细节与其他语言示例见 [`docs/worker-protocol.md`](docs/worker-protocol.md)。

## 是否需要注册中心

注册中心不应该管理池里的每个 worker；这些 worker 是单个池服务的内部实现。只有部署多个 `process-pool-server` 实例、调用方又没有固定入口时，才需要发现池服务实例。

- 单机、Unix 服务或固定地址：不需要注册中心。
- Kubernetes：可使用 Service，加 `/healthz` 和 `/readyz` 探针。应通过本地 `--config` 或管理通道先完成初始化，不要依赖只路由到 ready 实例的 Service 来完成首次初始化。
- 多台虚拟机或混合环境：可由部署层把池服务注册到 Consul/Nacos，并用 `/readyz` 做摘除判断。
- 跨节点可靠排队、任务持久化、崩溃后重投：这已是分布式任务队列问题，应接 Redis Streams、NATS JetStream、RabbitMQ 等，而不是把注册中心当任务队列。

因此当前实现提供注册中心所需的探针，但不绑定某一种注册产品。

## 故障语义与边界

- 每个 worker 同时只处理一个任务，不要求 worker 自己实现并发安全。
- 超时、进程退出、I/O 错误或协议错误会淘汰该 worker；已经激活的核心数量会补建，不会顺带创建尚未按需启动的核心名额。
- 初始化只验证配置结构和取值，不启动 worker，因此可执行文件缺失等启动错误会在第一次提交任务或显式预热时报告。预热不是原子操作；如果中途创建失败，已经创建的 worker 会保留。
- worker 返回结构化业务错误时进程仍可复用。
- 池不会自动重试任务，因为无法判断任务是否幂等。需要重试时应由业务方提供幂等键并在上层控制。
- 队列只在内存中，服务退出后不会恢复。
- stdout 响应目前按行读取，没有额外的消息大小参数；不要让 worker 返回无界数据。
- 对外网开放前必须增加 TLS、鉴权、请求大小限制和限流；默认监听 localhost 是有意的安全默认值。

## 开发验证

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
node --check web/dashboard.js
node --check web/rpc-client.js
node --check web/debugger.js
cargo build --bins
node --test tests/web_*.cjs
```

Web 测试包括调试表单逻辑、RPC 客户端错误处理，以及在随机本地端口启动独立测试池的真实调用流程；不会使用或重启你正在运行的 7788 实例，不等同于浏览器视觉验收。
