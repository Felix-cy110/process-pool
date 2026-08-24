# Rust 可复用进程池

这是一个受 Java `ThreadPoolExecutor` 启发的 Rust 进程池。它预启动核心子进程，通过 stdin/stdout 上的 NDJSON 协议反复向同一子进程发送任务，从而复用进程启动、运行时加载和初始化成本；外部调用方通过 HTTP JSON-RPC 2.0 提交 JSON 任务。

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

RPC 只负责执行任务，不提供远程初始化或修改 `process_factory` 的方法。允许远程用户设置 worker 命令等同于开放远程命令执行。7 个参数必须在受信任的服务启动阶段从本地配置读取。

## 7 个初始化参数

[`examples/pool-config.json`](examples/pool-config.json) 顶层严格包含下面 7 个参数：

| 参数 | Java 对应概念 | 本项目语义 |
| --- | --- | --- |
| `core_pool_size` | `corePoolSize` | 启动时预热并始终保留的核心进程数，可以为 0 |
| `maximum_pool_size` | `maximumPoolSize` | 池内受管理进程的最大数量，必须大于 0 |
| `keep_alive_time` | `keepAliveTime` | 非核心空闲进程的存活数值 |
| `time_unit` | `TimeUnit` | `milliseconds`、`seconds` 或 `minutes` |
| `work_queue` | `BlockingQueue` | 当前实现为有界队列；容量 0 等价于直接移交任务 |
| `process_factory` | `ThreadFactory` | worker 程序、参数、环境变量覆盖和工作目录 |
| `rejected_execution_handler` | `RejectedExecutionHandler` | `abort`、`discard`、`discard_oldest` 或 `caller_runs` |

调度顺序与 Java 线程池一致：

1. 有空闲 worker 时直接执行。
2. 少于核心数时创建 worker。
3. 核心 worker 都忙时先进入队列。
4. 队列满后再扩容，直到最大进程数。
5. 最大进程数也已忙时执行拒绝策略。

对 RPC 服务建议使用 `abort`，调用方会立即收到 `-32001` 并自行退避。`caller_runs` 在进程语义下会启动一个不计入 `maximum_pool_size` 的一次性进程，只是对 Java 策略的近似模拟，持续过载时可能产生大量进程。

## 快速开始

需要支持 Rust 2024 edition 的工具链；当前实现已在 Rust 1.97.1 上验证。

```bash
cd /Users/chenyang/process-pool
cargo build --bins
cargo run --bin process-pool-server -- --config examples/pool-config.json
```

默认只监听 `127.0.0.1:3000`。另开一个终端提交任务：

```bash
curl -sS http://127.0.0.1:3000/rpc \
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

再次调用时如果该 worker 空闲，响应里的 PID 保持不变，说明进程被复用。

查询池状态：

```bash
curl -sS http://127.0.0.1:3000/rpc \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"pool.stats","params":{}}'
```

健康与就绪接口：

```bash
curl -sS http://127.0.0.1:3000/healthz
curl -sS http://127.0.0.1:3000/readyz
```

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
- Kubernetes：优先使用 Service，加 `/healthz` 和 `/readyz` 探针，通常不需要应用自己注册。
- 多台虚拟机或混合环境：可由部署层把池服务注册到 Consul/Nacos，并用 `/readyz` 做摘除判断。
- 跨节点可靠排队、任务持久化、崩溃后重投：这已是分布式任务队列问题，应接 Redis Streams、NATS JetStream、RabbitMQ 等，而不是把注册中心当任务队列。

因此当前实现提供注册中心所需的探针，但不绑定某一种注册产品。

## 故障语义与边界

- 每个 worker 同时只处理一个任务，不要求 worker 自己实现并发安全。
- 超时、进程退出、I/O 错误或协议错误会淘汰该 worker；核心 worker 会补建。
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
```
