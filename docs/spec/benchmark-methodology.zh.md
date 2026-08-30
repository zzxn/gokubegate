> [English](./benchmark-methodology.md) | 简体中文

# gokubegate 基准测试方法（Benchmark Methodology）

状态：配套 `TestE2EModeBenchmark`（`test/e2e/harness/e2e_test.go`）与 e2e 报告 §5。
本文说明基准如何搭建、各指标如何监测与验收，供复现和扩展使用。

## 1. 目标

验证并比较 gokubegate 各模式/参数在典型生产负载下的表现，核心验收目标：

- 每个下游 Pod（1 CPU / 1 GiB，请求 10ms 内返回）承受 **5,000 QPS** 不出问题；
- 具体验收：零错误、p99 ≤ 10ms、各 Pod 流量均衡（5,000 ±30%）。

## 2. 测试环境

| 项目 | 值 |
| --- | --- |
| 集群 | kind v0.27.0，1 control-plane + 1 worker |
| 宿主 | Linux/amd64，24 核 / 30 GiB（与 Docker 伴随容器共享） |
| Go | go1.26.1 |
| 客户端位置 | 集群内 Kubernetes Job（in-cluster config） |
| 下游 | 4 副本 Deployment，requests/limits cpu=1、memory=1Gi |
| 下游实现 | `test/e2e/downstream`：返回 Pod identity JSON，暴露 `/stats` 累计计数 |

> 注意：kind 单 worker + 宿主共享资源，**绝对容量数字是环境相关观测**，用于同一轮内的
> 相对比较与验收，不是正式容量结论。

## 3. 被测配置矩阵

`TestE2EModeBenchmark` 中的 `steadyState` 列表定义了饱和与限速两个阶段共用的配置：

| 维度 | 取值 |
| --- | --- |
| 路由模式 | `pod`（默认）、`clusterip` |
| pod idle 预算 `MaxIdleConnsPerPod` | 8 / 16 / 32 / 64 / 128 / 512 |
| clusterip 轮换率 `1/denominator` | 0（关闭）/ 100 / 200 / 500 / 1000 |

扩容收敛阶段（`convergence` 列表）只覆盖 5 个 clusterip 轮换率（2→4 扩容）。

每个配置使用**独立 tester Job**（全新进程、干净连接池），互不污染；clusterip 配置固定
idle=16 以隔离轮换率变量。

## 4. 架构与数据流

```text
harness (go test -tags e2e)
  ├─ 建 kind 集群 / build+load 镜像 / apply manifests
  ├─ 调度 tester Job，等待完成并解析结果 JSON
  └─ 用 kubectl exec 读 downstream /stats（服务端权威计数）

tester (in-cluster Job, test/e2e/tester)
  ├─ 创建 gokubegate Client（-gate-mode / -connection-close-denominator /
  │   -max-idle-conns / -rate / -concurrency / -duration）
  ├─ N 个并发 worker 发请求（HTTP，可限速）
  └─ 采集客户端侧指标（请求结果、Hook 事件、每秒窗口）→ 输出一行 JSON

downstream (4 × 1c1g Deployment)
  └─ 每个请求返回 {pod, host, path, query}，/stats 返回累计计数
```

数据流：请求 → gokubegate Client（picker/transport）→ downstream；tester 从响应体取
Pod 名（byPod），从 Hook 事件取连接复用/轮换/endpoint 指标；harness 读服务端计数做
交叉验证。

## 5. 三个阶段

| 阶段 | 参数 | 时长 | 目的 |
| --- | --- | --- | --- |
| 1. 饱和 | 128 并发，不限速 | 12s | 各配置可达最大吞吐、饱和延迟、连接复用 |
| 2. 限速验收 | `-rate 20000`，128 并发 | 10s | 精确以 5,000 QPS/Pod 施压，验收零错误/p99/均衡 |
| 3. 扩容收敛 | 2→4 副本，128 并发 | 18s | 新 Pod 首次接流量延迟与占比（clusterip） |

## 6. 指标定义与采集

### 6.1 客户端侧（tester → `TesterResult` JSON）

| 指标 | 字段 | 来源 |
| --- | --- | --- |
| 请求/成功/错误 | `requests` / `success` / `errors` | 每个请求逐一计数 |
| 错误分类 | `errorKinds` / `errorSamples` | `classifyError`：connection_refused / connection_reset / no_endpoints / timeout / eof / other |
| 吞吐 | `throughput` | success / 负载耗时 |
| 延迟 | `latencyP50/P95/P99/MaxMs` | 请求往返耗时样本百分位 |
| 连接复用 | `reused` / `reusedRatio` | Hook `EventRequestDone.Reused` |
| 请求总数 | `connections` | `EventRequestDone` 次数 |
| 轮换次数 | `rotations` | Hook `EventConnectionRotated`（clusterip） |
| 分布 | `byEndpoint` / `byPod` | `EventEndpointPicked` / 响应体 Pod 名 |
| 每秒窗口 | `windows[]` | 每秒 requests/success/errors/latencyP95/byPod，短窗口不被聚合掩盖 |
| endpoint 时间线 | `endpointUpdates[]` | `EventEndpointsUpdated`（ready/draining） |

### 6.2 服务端侧（downstream `/stats`）

阶段前后各读一次每 Pod 累计计数（harness `PodStats`），差值 / 时长 = 每 Pod 实际 QPS。
这是权威计数，与客户端 `byPod` 交叉验证（如饱和阶段两者完全一致）。

### 6.3 限速实现（tester `-rate`）

每个 worker 独立 pacing：

```text
paceInterval = 1s × concurrency / rate
第 i 个请求的发送时刻 = workerStart + i × paceInterval
```

`-rate` 需配合 `-duration` 使用；实际吞吐达不到目标时（如 clusterip 饱和），worker
自然全速发送，`achieved` 会低于目标——这本身就是"该配置无法达标"的信号。

### 6.4 扩容收敛度量

从 `windows[].byPod` 计算每个新 Pod 首次出现在流量中的秒数（firstSeen），对比
EndpointSlice 就绪时刻（harness 记录 `readyAt`），得到 `delayAfterReady`；18s 窗口内
新 Pod 的请求占比衡量收敛充分度。

## 7. 验收标准（测试断言）

限速验收阶段（`rate-*` 子用例）：

- `pod` 模式：`errors == 0` 且 `achieved >= target × 0.95` 且 `p99 <= 10ms` 且每 Pod
  QPS ∈ [3,500, 6,500]（5,000 ±30%），任一不满足即该子用例失败；
- `clusterip`：只记录 MET/NOT-MET 状态（探索性配置），不硬失败；
- 至少一个 pod 配置达标，否则整个测试失败（`rateMet == 0`）。

饱和/收敛阶段：错误只记录不失败——clusterip 高并发下偶发 `cannot assign requested
address`（dial ClusterIP 时客户端本地地址分配失败，<0.01%，疑似 conntrack/环境噪音）。

## 8. 运行方法

```bash
# 完整基准（饱和 + 限速 + 收敛，约 10 分钟，27 个子用例）
make e2e ARGS='-run TestE2EModeBenchmark'

# 只跑限速验收（跳过饱和/收敛）
GOKUBEGATE_E2E_BENCH_RATE_ONLY=1 make e2e ARGS='-run TestE2EModeBenchmark'

# 保留集群便于排查（结束后需手动 kind delete cluster --name gokubegate-e2e）
GOKUBEGATE_E2E_KEEP=1 make e2e ARGS='-run TestE2EModeBenchmark'

# 观察运行中进度（测试输出重定向到文件后 tail）
GOKUBEGATE_E2E_KEEP=1 go test -tags e2e -timeout 30m -v \
  -run TestE2EModeBenchmark ./test/e2e/harness/... > /tmp/bench.log 2>&1
```

每个子用例的结论行（`bench ...` / `rate ...` / `convergence ...`）即报告 §5 表格的数据源。

## 9. 数据边界与注意事项

- kind 绝对容量受宿主与伴随容器影响；**同一轮内的配置对比是可靠的，跨轮数字有随机波动**
  （如 clusterip 饱和吞吐多轮在 10k–59k req/s 间波动，与连接池建连时机有关）。
- `idle64` 等个别档位可能出现瞬时 p99 尖峰（如 9.27ms），结论应基于多档位趋势而非单点。
- 快速缩容竞态（拨到已终止 Pod 的瞬时错误，两模式均有、秒级窗口）不在本基准覆盖，
  见 e2e 报告 §3 快速扩缩容场景。
- 本基准只测 HTTP 短请求；SSE/长流、大响应、资源受限下的表现需另行设计（见 §10）。

## 10. 如何扩展新档位/新配置

1. 编辑 `TestE2EModeBenchmark` 中的 `steadyState`（饱和/限速共用）与 `convergence`
   列表，新增 `benchConfig{name, mode, denominator, maxIdle}` 条目；
2. tester 无需改动（`-max-idle-conns` / `-connection-close-denominator` /
   `-rate` 均已参数化）；
3. 若要调整目标（如 8,000 QPS/Pod），改 `targetQPSPerPod` 与 `targetTotal`，并把
   `-rate` 传参改为新目标值；
4. 新增指标：在 tester 的 `metrics` 与 `TesterResult`、harness 的 `TesterResult`
   （JSON 镜像）同步加字段即可。
