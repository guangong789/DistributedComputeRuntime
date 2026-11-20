简体中文 | [English](README.en.md)

# Distributed Compute Runtime

**具备运维诊断的可靠异构计算运行时**

这是一个用 Go 实现的可靠分布式计算运行时，采用单 Coordinator、多 Worker 架构，通过 gRPC/mTLS 通信，并使用 SQLite 持久化。支持内置 hash、sleep 任务，以及受信任的外部 CPU 和 CUDA 程序。

## 架构

~~~mermaid
flowchart LR
    client["Client"] -->|"gRPC / mTLS"| coordinator["Coordinator"]
    coordinator <-->|"gRPC / mTLS"| workers["Workers"]
    coordinator <--> sqlite["SQLite"]
    workers --> compute["受信任的 Linux 计算程序"]
~~~

- **Coordinator** 负责持久化接纳、按能力调度、Worker 注册会话和心跳存活检测，以及 Attempt 授权、租约、重试和恢复。
- **Worker** 执行内置任务，并负责外部进程的容量限制、运行和清理。
- **Client** 提交任务后，通过 GetJob 查询持久化结果。CLI 默认提交 hash 任务，也可用[仅查询模式](cmd/client/execution.go)重新连接并查询已有 JobID。

## 持久化与结果接纳

- **任务接纳。** SubmitJob 持久化 Queued 任务，返回 Accepted、原 JobID 和 AttemptID 0。后续执行由 Coordinator 管理，独立于提交 RPC 的生命周期。重复提交已有 JobID 不会创建第二个任务。JobSpec 不可变，按原始存储值精确比较。GetJob 返回当前持久化状态及已接纳的结果。
- **Attempt 与 fencing。** Coordinator 确认 Attempt 已持久化后才派发任务。它核对响应的 JobID 和 AttemptID，并在写入终态时再次检查上下文和租约。已过期或被取代的 Attempt 无法覆盖更新的结果。[Attempt 执行](cmd/coordinator/attempt_execution.go)
- **重启与不确定写入。** 启动时，符合条件的 Running 任务转为 Recovering，保留原 JobSpec 和 AttemptID。下次派发的 AttemptID 高于已有代次。Coordinator 发现 Queued 和 Recovering 任务，并在重试前依据 SQLite 对不确定写入进行协调。[Attempt 分配](cmd/coordinator/attempts.go) · [写入协调](cmd/coordinator/queued_session.go)

## 支持的任务

| TaskType | 执行方式 |
|---|---|
| `hash` | 对非空输入执行内置 SHA-256 |
| `sleep` | 按 Go duration 等待，支持取消 |
| `external_square` | 受信任的 Linux CPU 程序，使用有界整数协议 |
| `cuda_vector_transform` | 独立 CUDA 程序，使用固定向量协议，由 CPU 独立校验结果 |

在 Linux 上，两类外部任务共用一个本地执行名额，以直接子进程运行，并启用父进程退出保护。取消或超时时先发 SIGTERM，等待 300 ms 后可升级为 SIGKILL。Worker 在子进程回收和 I/O 清理完成后才归还名额。正常关闭会在配置的时限内等待已接纳的处理程序退出。若超过时限，Worker 会以错误状态退出。内核或驱动卡住时，清理可能无法完成。构建与配置见 [CPU 任务说明](cmd/external-square/README.md)和 [CUDA 任务说明](cmd/cuda-vector/README.md)。

## 安全与可观测性

TLS 1.3 和 mTLS 认证对端身份。RPC 授权检查角色，将 WorkerID 与证书身份绑定，并验证 Worker 公告地址。Coordinator 提供接纳、Job、Attempt、Worker 存活状态和恢复的 Prometheus 指标。CUDA 失败可在 Worker 日志中记录固定的故障阶段。该阶段只描述本地执行，不携带子进程的原始输出。

Coordinator 和 Worker 各自通过独立的无认证明文 HTTP 端点提供指标，默认绑定回环地址。Worker 的指标包括[本地执行名额和探针](cmd/cuda-vector/README.md#worker-compute-metrics)。这两个端点位于 gRPC mTLS 的保护范围之外。向不受信任的网络开放前，须配置外部访问控制。

### 只读诊断

现有 Coordinator mTLS 监听端口默认拒绝诊断请求。使用 `-allow-client` 和 `-allow-diagnostics-client` 同时授权同一个 Client 证书身份。普通 GetJob 权限不包含诊断权限。Worker 和 Coordinator 身份不具备该权限。

命令格式如下，方括号与尖括号内是占位符。

~~~text
client [flags] workers
client [flags] diagnose job <signed-job-id>
~~~

在子命令前用 `-coordinator` 指定目标，并提供 `-tls-ca`、`-tls-cert` 和 `-tls-key`。`workers` 调用一次 `ListWorkers`，`diagnose job` 调用一次 `DiagnoseJob`。每次调用的时限最多为两秒。参数错误会在建立连接前被拒绝。查询失败时以非零状态退出，不输出成功快照。

`workers` 显示 Coordinator 采样时间、记录的存活状态、已配置的外部任务、当前可调度的任务和 CUDA 有效健康状态及新鲜度。`diagnose job` 显示持久化状态、记录的 Attempt 和 Worker、是否存在已接纳的 Worker 结果，以及已配置且兼容、兼容且记录为 Alive、以及当前符合调度条件的 Worker 数量。如果复查发现持久化 Job 记录已变化或不存在，`JOB_CHANGED` 优先于按状态给出的解释。若两次读取一致，`Running` 的当前 Attempt 授权显示为 `Unknown`，终态任务的放置解释为 `Not Applicable`。已过期的托管健康证据会显示为 `Unknown`，并标记为 `Expired`。

诊断无法确定历史排队原因、远程 Worker 当前的执行名额归属或未来的调度进展。Job 读取、Worker 注册表快照和 Worker 指标各有独立的采样边界，无法组成全局原子快照。

## 快速开始

在 Linux 或 WSL 上从仓库根目录运行（需 OpenSSL 3 和本地 Go 依赖缓存）。

~~~sh
go run -mod=readonly ./cmd/demo
go run -mod=readonly ./cmd/demo -show-cli
~~~

第一条运行 `hash` 演示，核验 `GetJob` 结果与 SQLite 记录后清理临时环境。第二条还使用临时获授权的 mTLS 凭据，显示[只读诊断](#只读诊断)中 `workers` 和 `diagnose job` 的实际输出。

## 设计决策与运行边界

### 执行代次、租约与结果隔离

持久化的 `AttemptID` 标识执行代次，租约限定该代次的授权时段。Coordinator 写入终态前再次核对代次与租约，防止已过期或被取代的 Attempt 覆盖新代次已接纳的结果。

### 为什么诊断无法还原历史排队原因？

候选数量取自 Coordinator 的采样状态。Job 读取和 Worker 注册表快照没有全局原子性。若复查发现记录已变化或消失，优先返回 `JOB_CHANGED`。当前有合格的 Worker 也无法说明任务此前为何排队。

### 单 Coordinator 与故障恢复边界

一个 Coordinator 管理 Worker 注册会话、执行授权和恢复协调。SQLite 保存 Job 状态，Coordinator 重启后可据此恢复符合条件的未完成任务。该部署范围没有备用 Coordinator 自动接管。

### 为什么 Alive 的 Worker 仍可能无法执行 CUDA 任务？

`Alive` 表示 Coordinator 记录的存活状态，实际可达性仍需另行确认。CUDA 放置还要求已配置的能力。托管模式要求当前会话中已被 Coordinator 接纳且未过期的 `Ready` 证据。标准二进制采用非托管模式。

### 外部计算与健康探针共享执行名额

外部 CPU、CUDA 任务与显式调用的一次性 CUDA 探针共用 Worker 的一个本地名额，因此会竞争容量。进程回收、I/O 清理和必要的结果校验完成后才归还名额。名额仅约束本地并发，既不隔离 GPU，也不代表 Coordinator 已预留容量。

## 里程碑范围与验证

本次单 Coordinator 里程碑涵盖上述执行与诊断功能。基于已接纳健康证据的托管 CUDA 放置，以及显式调用的一次性探针，均已在受控环境中验证。标准二进制采用非托管 CUDA 放置。参见[CUDA 健康检查与探针的边界](cmd/cuda-vector/README.md#cuda-health-and-probing)。

暂缓的功能包括生产环境的周期探针生命周期、无人值守的托管 CUDA 启用、基于 GPU 利用率的调度、动态容量调度、远程 Worker 执行名额查询、全局指标聚合、历史诊断事件存储、Kubernetes 集成、AI 模型服务、多 Coordinator 共识，以及交互式或全屏 TUI。

I0-01 在当时的审查快照上验证了分层构建及聚焦的常规和 Race 测试，并复用了同一实现的 Fast（78 项通过）、Core（860）、Full（1,362）、Race（860）、真实进程 Hash 演示及 mTLS CLI 冒烟测试。Core、Full 和 Race 各跳过一项需显式启用的 GPU 测试。RTX 3060 上的一次性探针曾在受控环境中验证全部 64 个输出，I0-01 也复用了该记录。故障分类由自动化测试覆盖。实际 GPU 驱动故障下的恢复及生产规模下的表现仍未验证。完整回归和 Race 验证见[Go 测试工作流](scripts/README.md)。

## 历史数据库

新提交的 `zk_preimage_prove` 任务会在持久化之前以 `FailedPrecondition` 拒绝。若 SQLite 数据库包含这种已退役任务的 Queued、Running 或 Recovering 记录，Coordinator 会在初始化表结构或执行恢复写入前拒绝启动。可用兼容的旧版本完成未结束任务，也可改用其他数据库。原记录会保留，历史终态记录仍可通过 GetJob 查询。

[GitHub 仓库](https://github.com/guangong789/DistributedComputeRuntime)和 Go module 使用 `github.com/guangong789/DistributedComputeRuntime`。
为兼容已有证书和监控配置，部署继续使用 SPIFFE 信任域 `distributedzk.local`、现有开发 CA 主题和 Prometheus 指标前缀 `dzkr_`。
