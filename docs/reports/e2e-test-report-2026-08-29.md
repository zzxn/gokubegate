# gokubegate 端到端测试报告

## 1. 结论

本轮在真实 kind Kubernetes 集群中完成 S1–S5 全量验证，6 个测试场景全部通过。
正常权限下共发出 1,900 个 HTTP 请求，成功 1,900 个、业务错误 0 个；扩容、缩容、
NotReady 摘除和恢复后的流量分布都与当前 Ready EndpointSlice 集合严格一致。

核心结论：

- 两个 Pod 时 500 个请求精确分为 250/250；四个 Pod 时精确分为 125/125/125/125。
- 已经缩容或变为 NotReady 的 Pod 不再收到新请求。
- NotReady Pod 恢复后重新加入轮询，200 个请求精确分为 50/50/50/50。
- 稳定态各压力阶段均为零错误，连接复用率为 98.0%–99.6%。
- 无 RBAC 权限时，`NewClient` 在启动阶段失败，没有退化为绕过权限或带病运行。
- Host、用户指定 Host、path 和 query 均端到端保真。

## 2. 测试环境

| 项目 | 值 |
| --- | --- |
| 日期 | 2026-08-29 |
| 代码基线 | `7426949` 加当前工作区 e2e 实现 |
| 操作系统/架构 | Linux / amd64 |
| Go | go1.26.1 |
| Docker Client/Server | 29.1.2 / 29.1.2 |
| kind | v0.27.0 |
| kubectl client | v1.34.1 |
| 集群拓扑 | 1 control-plane + 1 worker |
| 下游服务 | HTTP，初始 2 副本，测试中扩至 4 副本 |
| 客户端位置 | 集群内 Kubernetes Job，使用 in-cluster config |

执行命令：

```bash
GOCACHE=/tmp/gokubegate-go-cache make e2e
```

报告采集轮完整套件耗时 96.472 秒。此前一次完整验收运行耗时 148.731 秒，两次均全部
通过；差异主要受 kind、Go 和 Docker 构建缓存影响。测试完成后已确认 `No kind clusters found`。

## 3. 场景结果

### S1：基础负载均衡

| 指标 | 结果 |
| --- | --- |
| 请求 | 500 |
| 成功 / 错误 | 500 / 0 |
| 客户端请求阶段耗时 | 228 ms |
| 观测吞吐 | 2,193.0 req/s |
| 连接复用率 | 99.6% |
| Hook 分布 | Pod A: 250，Pod B: 250 |
| Pod 侧计数差值 | Pod A: 250，Pod B: 250 |

Hook 与下游 Pod 自身计数完全一致，证明请求并非只在客户端侧“被选择”，而是确实到达了
对应 Pod。两个 Pod 各占 50%，优于测试要求的 40%–60% 容差。

### S2：Host、路径和查询参数保真

| 请求方式 | 下游收到的 Host | Path | Query | 结果 |
| --- | --- | --- | --- | --- |
| 默认 Host | `downstream.gokubegate-e2e.svc.cluster.local:8080` | `/api/items` | `page=1` | 50/50 成功 |
| 自定义 Host | `custom.example.com:8080` | `/api/items` | `page=1` | 50/50 成功 |

默认情况下，gokubegate 只把实际拨号地址改为 Pod IP，HTTP Host 仍保持逻辑 Service 域名；
调用方显式设置 `req.Host` 时，该值也得到尊重。

### S3：从 2 个 Pod 扩容到 4 个 Pod

| 指标 | 结果 |
| --- | --- |
| 请求 | 500 |
| 成功 / 错误 | 500 / 0 |
| 客户端请求阶段耗时 | 200 ms |
| 观测吞吐 | 2,500.0 req/s |
| 连接复用率 | 99.2% |
| 分布 | 125 / 125 / 125 / 125 |

等待 Deployment 与 EndpointSlice 都收敛到 4 个 Ready endpoint 后，新 Pod 已进入请求级
轮询。每个 Pod 占 25%，显著高于“每个新 Pod 至少 5%”的验收下限。

### S4-A：从 4 个 Pod 缩容到 2 个 Pod

| 指标 | 结果 |
| --- | --- |
| 请求 | 300 |
| 成功 / 错误 | 300 / 0 |
| 客户端请求阶段耗时 | 138 ms |
| 观测吞吐 | 2,173.9 req/s |
| 连接复用率 | 99.3% |
| 存活 Pod 分布 | 150 / 150 |
| 已移除 Pod 命中 | 0 |

EndpointSlice 收敛后，流量集合只包含两个存活 Pod，没有请求继续选择已删除 endpoint。

### S4-B：Pod NotReady 与恢复

NotReady 阶段：

| 指标 | 结果 |
| --- | --- |
| 请求 | 300 |
| 成功 / 错误 | 300 / 0 |
| 客户端请求阶段耗时 | 136 ms |
| 观测吞吐 | 2,205.9 req/s |
| 连接复用率 | 99.0% |
| Ready Pod 分布 | 100 / 100 / 100 |
| NotReady Pod Hook 命中 | 0 |
| NotReady Pod 计数变化 | 0 → 0，无增长 |

恢复阶段：

| 指标 | 结果 |
| --- | --- |
| 请求 | 200 |
| 成功 / 错误 | 200 / 0 |
| 客户端请求阶段耗时 | 87 ms |
| 观测吞吐 | 2,298.9 req/s |
| 连接复用率 | 98.0% |
| 四 Pod 分布 | 50 / 50 / 50 / 50 |

NotReady Pod 在控制面 EndpointSlice 状态收敛后完全停止接收新请求；恢复 Ready 后重新进入
轮询，且没有出现恢复后偏斜。

### S5：RBAC

默认最小权限 ServiceAccount 成功完成上述所有请求。无 RoleBinding 的 ServiceAccount 在
`NewClient` 解析 Service 端口时收到 Kubernetes `forbidden`：

```text
NewClient: gokubegate: resolve port for gokubegate-e2e/downstream:
services "downstream" is forbidden: User
"system:serviceaccount:gokubegate-e2e:gokubegate-tester-denied"
cannot get resource "services" in the namespace "gokubegate-e2e"
```

从创建失败 Job 到 harness 观测到失败共 4.053 秒，其中包含 Pod 调度、启动和 1 秒状态轮询，
不代表 `NewClient` 自身耗时。应用进程是在启动阶段直接失败，未发送下游请求。

## 4. 汇总观察

| 稳定态 | Endpoint 数 | 请求数 | 分布 | 错误 | 复用率 | 观测 req/s |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| 基线 | 2 | 500 | 250 / 250 | 0 | 99.6% | 2,193.0 |
| 扩容后 | 4 | 500 | 125 × 4 | 0 | 99.2% | 2,500.0 |
| 缩容后 | 2 | 300 | 150 / 150 | 0 | 99.3% | 2,173.9 |
| 一个 Pod NotReady | 3 | 300 | 100 × 3 | 0 | 99.0% | 2,205.9 |
| Pod 恢复 | 4 | 200 | 50 × 4 | 0 | 98.0% | 2,298.9 |

精确均分来自默认 round-robin picker 和串行请求模型，符合实现预期。Endpoint 数变化时，首次
连接会为每个新 Pod 建立独立连接池，因此 4 Pod、200 请求的恢复阶段复用率相对最低，但仍有
98.0%。

## 5. 数据边界与后续建议

本报告中的 req/s 是单个集群内 tester 串行发请求、下游返回小 JSON 时的功能测试附带观测，
用于比较各生命周期状态是否出现明显退化，不是正式容量结论。它不包含 `NewClient` 初始化、
Job 调度或 Deployment/EndpointSlice 收敛时间，也没有覆盖并发请求、响应大包、慢请求和资源
限制下的表现。

后续建议按优先级补充：

1. 并发梯度测试（1、10、50、100 goroutine），记录 p50/p95/p99、吞吐和错误率。
2. 在持续流量中执行扩缩容和 readiness 切换，量化控制面传播窗口内的错误与尾延迟。
3. 增加 SSE 长连接，验证已有流固定 Pod、新流继续均衡和 drain 行为。
4. 模拟 API Server watch 中断，验证 last-known-good 快照及恢复后的重新同步。
5. 将正式性能测试与当前正确性 e2e 分开，避免共享宽松超时或把环境缓存误计入结果。

## 6. 验证状态

- `go test ./...`：通过。
- `go vet ./...`：通过。
- `go test -c -tags e2e ./test/e2e/harness`：通过。
- 两轮完整真实集群 e2e：全部通过。
- `git diff --check`：通过。
- 临时 kind 集群：已清理。
