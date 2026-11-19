简体中文 | [English](README.en.md)

# Distributed Compute Runtime

这是一个用 Go 实现的可靠分布式计算运行时，采用单 Coordinator、多 Worker 架构，通过 gRPC/mTLS 通信，并使用 SQLite 持久化。支持内置 hash、sleep 任务，以及受信任的外部 CPU 和 CUDA 程序。

## 架构

~~~mermaid
flowchart LR
    client["Client"] -->|"gRPC / mTLS"| coordinator["Coordinator"]
    coordinator <-->|"gRPC / mTLS"| workers["Workers"]
    coordinator <--> sqlite["SQLite"]
    workers --> compute["受信任的 Linux 计算程序"]
~~~

- **Coordinator** 负责持久化接纳、调度、Worker 注册和存活检测，以及 Attempt 授权、租约、重试和恢复。
- **Worker** 执行内置任务，并负责外部进程的容量限制、运行和清理。
- **Client** 提交任务后，通过 GetJob 查询持久化结果。CLI 默认提交 hash 任务，也可用[仅查询模式](cmd/client/execution.go)重新连接并查询已有 JobID。

## 持久化与结果接纳

- **任务接纳。** SubmitJob 持久化 Queued 任务，返回 Accepted、原 JobID 和 AttemptID 0。后续执行由 Coordinator 管理，独立于提交 RPC 的生命周期。持久化 JobID 唯一，JobSpec 按原始存储值精确比较。GetJob 返回当前持久化状态及已接纳的结果。
- **Attempt 与 fencing。** Coordinator 确认 Attempt 已持久化后才派发任务。它核对响应的 JobID 和 AttemptID，并在写入终态时再次检查上下文和租约。已过期或被取代的 Attempt 无法覆盖更新的结果。[Attempt 执行](cmd/coordinator/attempt_execution.go)
- **重启与不确定写入。** 启动时，符合条件的 Running 任务转为 Recovering，保留原 JobSpec 和 AttemptID。下次派发的 AttemptID 高于已有代次。Coordinator 发现 Queued 和 Recovering 任务，并在重试前依据 SQLite 对不确定写入进行协调。[Attempt 分配](cmd/coordinator/attempts.go) · [写入协调](cmd/coordinator/queued_session.go)

## 支持的任务

| TaskType | 执行方式 |
|---|---|
| `hash` | 对非空输入执行内置 SHA-256 |
| `sleep` | 按 Go duration 等待，支持取消 |
| `external_square` | 受信任的 Linux CPU 程序，使用有界整数协议 |
| `cuda_vector_transform` | 独立 CUDA 程序，使用固定向量协议，由 CPU 独立校验结果 |

两类外部任务共用一个本地执行名额。取消时先发 SIGTERM，宽限期结束后如有必要再发 SIGKILL。Worker 在子进程回收和 I/O 清理完成后才归还名额，关闭时等待已接纳的处理程序退出。构建与配置见 [CPU 任务说明](cmd/external-square/README.md)和 [CUDA 任务说明](cmd/cuda-vector/README.md)。

## 安全与可观测性

TLS 1.3 和 mTLS 认证对端身份。RPC 授权检查角色，将 WorkerID 与证书身份绑定，并验证 Worker 公告地址。Coordinator 提供接纳、Job、Attempt、Worker 存活状态和恢复的 Prometheus 指标。CUDA 失败可在 Worker 日志中记录固定的故障阶段。该阶段只描述本地执行，不携带子进程的原始输出。

## 快速开始

在 Linux 或 WSL 中，从仓库根目录运行本地演示。

~~~sh
go run -mod=readonly ./cmd/demo
~~~

演示使用 [go.mod](go.mod) 指定的工具链、Bash、OpenSSL 3、Git、GNU 工具及本地缓存的 Go 依赖。它以隔离的开发 mTLS 配置启动真实 Coordinator 和 Worker，提交 hash 任务，并独立核对 GetJob 结果与 SQLite 记录。结束时回收子进程并清理临时目录。

手动启动时可使用[开发 PKI 脚本](scripts/dev-pki/bootstrap.sh)，也可提供受信任 CA 和匹配的证书。三个程序均需配置 `-tls-ca`、`-tls-cert` 和 `-tls-key`。Coordinator 还需要 Client、Worker 身份允许列表和可写的 SQLite 路径。Worker 需要可达的 Coordinator 地址，以及与证书匹配的公告地址。可运行 `go run -mod=readonly ./cmd/coordinator -help` 或对应的 `worker`、`client` 命令查看参数。

~~~sh
go build -mod=readonly ./...
make test-fast
~~~

完整回归和 Race 验证见 [Go 测试工作流](scripts/README.md)。

## 历史数据库

新提交的 `zk_preimage_prove` 任务会在持久化之前以 `FailedPrecondition` 拒绝。若 SQLite 数据库包含这种已退役任务的 Queued、Running 或 Recovering 记录，Coordinator 会在初始化表结构或执行恢复写入前拒绝启动。可用兼容的旧版本完成未结束任务，也可改用其他数据库。原记录会保留，历史终态记录仍可通过 GetJob 查询。

[GitHub 仓库](https://github.com/guangong789/DistributedComputeRuntime)和 Go module 使用 `github.com/guangong789/DistributedComputeRuntime`。
为兼容已有证书和监控配置，部署继续使用 SPIFFE 信任域 `distributedzk.local`、现有开发 CA 主题和 Prometheus 指标前缀 `dzkr_`。
