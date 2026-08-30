> [English](./e2e-integration-test-plan.md) | 简体中文

# gokubegate 真实集群集成测试实现计划

## 文档状态

- 状态：Plan
- 日期：2026-08-29
- 范围：`test/e2e` 真实 Kubernetes 集群集成测试（基于 kind + Docker）
- 本文是实施计划，对应设计文档 [technical-design.md](./technical-design.md) 的 13.2 集成测试部分

## 1. 背景与目标

单元测试用 fake clientset 验证了 informer/聚合/Picker 等内部逻辑，但无法覆盖：

- 真实 kube-apiserver + controller-manager 产生的 EndpointSlice（controller 语义、条件字段、删除时序）；
- 真实 Pod 网络、Service/endpoint 扩缩容、readiness 变化；
- 集群内运行时的 in-cluster config、ServiceAccount、RBAC 真实行为。

本计划用 **kind**（Kubernetes in Docker）在 Docker 内启动一个真实 K8s 集群，把测试组件（下游服务、压测客户端）全部容器化，测试结束整体删除集群，**不污染宿主机环境**（不修改现有集群配置、不占用宿主机端口、不留运行中的服务）。

### 验收目标（场景总览）

| 编号 | 场景 | 一句话验收标准 |
| --- | --- | --- |
| S1 | 基础负载均衡 | 2 副本下游，500 请求按请求级分发，各 Pod 命中在容差内 |
| S2 | Host/路径保真 | 下游收到的 Host 为逻辑 Service 域名，path/query 原样 |
| S3 | 扩容 | 2→4 副本，新 Pod Ready 后开始收到流量 |
| S4 | 缩容 / NotReady | 摘除/NotReady 的 Pod 不再收到新请求 |
| S5 | RBAC | 最小权限可运行；缺权限时 `NewClient` 启动即失败 |
| S6 | SSE 长连接（可选） | 流建立后固定在一个 Pod，新流继续均衡 |
| S7 | API Server 抖动（可选） | informer 断连期间继续用 last-known-good 转发 |

## 2. 非目标（本期不做）

- 性能/容量压测（单独 benchmark，见设计文档 13.3）；
- 服务网格（Istio/Linkerd）下的 mTLS/sidecar 路径验证；
- 多集群、跨地域；
- HTTP/2 验证（v0.1 未启用）；
- Windows/macOS 之外的平台适配（本机为 Linux；Docker Desktop 可运行 kind）。

## 3. 技术选型

### 3.1 为什么用 kind

- **真实集群**：kind 启动的是真实 kube-apiserver、kube-controller-manager、kubelet、CNI（kindnet），EndpointSlice 由真实 controller 生成，语义与生产一致；
- **全容器化**：控制面与节点都是 Docker 容器，`kind delete cluster` 可整体清理；
- **官方维护**：kubernetes-sigs 出品，CI 友好，有官方 GitHub Action；
- **镜像可预加载**：`kind load docker-image` 直接加载本地构建镜像，无需 registry。

备选：k3d（k3s in Docker）更轻更快，但 k3s 使用自带 Service 代理与精简组件，EndpointSlice/readiness 行为与标准 K8s 有差异；作为备选记录，默认用 kind。

### 3.2 为什么不用 envtest

envtest 只启动 kube-apiserver + etcd 二进制，没有 kubelet/节点/Pod/网络，无法验证真实 Pod 与 EndpointSlice 生命周期，不满足"真实集群"要求。

### 3.3 Docker 隔离策略

- kind 集群：全部组件运行在 Docker 容器内，不占宿主机端口（kind 自动分配），不动宿主机 kubeconfig（测试用临时 kubeconfig，放在 `.cache/e2e/`）；
- 测试镜像：`downstream`、`tester` 两个镜像本地构建 + `kind load`，无需推送 registry；
- 清理：`t.Cleanup` / `make e2e-clean` 保证删除 namespace、Job、Deployment；`kind delete cluster` 删除集群；`GOKUBEGATE_E2E_KEEP=1` 保留集群便于调试；
- 全隔离模式（可选增强）：`make e2e-dind` 在 Docker 容器内（挂载 docker.sock）运行整套测试，宿主机只需 Docker，连 kind/kubectl/go 都不需要安装。

### 3.4 镜像构建

- `downstream`：multi-stage，`golang:1.26` 构建 → `alpine`/`distroless` 运行；
- `tester`：同样 multi-stage，import `github.com/zzxn/gokubegate`，作为 Job 运行；
- 构建缓存：利用 `docker build` 层缓存；镜像 tag 用短 commit sha，避免陈旧。

## 4. 环境要求

| 依赖 | 版本/说明 | 本机现状 |
| --- | --- | --- |
| Docker | >= 24 | ✅ 29.1.2 (Docker Desktop) |
| kind CLI | v0.2x（计划固定一个版本） | ❌ 未安装 → 引导脚本自动下载到 `.cache/kind/` |
| kubectl | 可选（driver 用 client-go 也行） | ✅ 已有 |
| Go | 1.26+（driver 与镜像构建用） | ✅ 1.26.1 |
| 内存 | kind 单集群建议 >= 4GB | ✅ 30GB |

引导脚本 `scripts/e2e-bootstrap.sh`：

```bash
# 下载 kind 到 .cache/kind/（不装进系统），并校验 sha256
KIND_VERSION=${KIND_VERSION:-v0.27.0}
curl -Lo .cache/kind/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64"
chmod +x .cache/kind/kind
```

## 5. 目录结构与组件

```text
test/e2e/
  driver_test.go        // 主入口：//go:build e2e 门控；kind 生命周期 + 场景编排 + 断言
  harness/
    kind.go             // 创建/删除/复用 kind 集群、获取临时 kubeconfig
    kube.go             // client-go 封装：apply manifests、wait Ready、scale、读日志
    wait.go             // 轮询等待（超时可配）
  downstream/
    main.go             // 下游 HTTP 服务：返回 Pod 名/IP；/ready 可切换 readiness；/stats 计数
    Dockerfile
  tester/
    main.go             // 使用 gokubegate 的压测 CLI：跑 N 请求，输出 JSON 结果
    Dockerfile
  manifests/
    namespace.yaml      // gokubegate-e2e
    rbac.yaml           // ServiceAccount + Role(endpointslices rw + services get) + RoleBinding
    rbac-denied.yaml    // 只有错误 Role 的副本（S5 用）
    downstream.yaml     // Deployment(副本数可覆盖) + Service + readinessProbe
    tester.yaml         // Job 模板（命令参数化：请求数/目标 URL/输出）
  README.md             // 如何运行、常见问题
Makefile                // make e2e / e2e-e2e-down / e2e-clean / e2e-dind
scripts/
  e2e-bootstrap.sh      // 下载 kind 到 .cache
  e2e-dind.sh           // 可选：全隔离 DinD 入口
```

### 5.1 downstream（下游被测服务）

- 监听 8080（与 Service/EndpointSlice 端口一致）；
- `GET /`：返回 `{pod, node, ip, host}`（`pod` 从 `POD_NAME` env / 自身 hostname 读取）；
- `GET /ready`：返回 readiness 状态（可被 `/admin/ready?on=false` 切换，供 S4 模拟 NotReady）；
- `GET /stats`：返回本 Pod 收到的请求数（内存计数，含 `x-gokubegate-count` 之类可关闭的日志）。

### 5.2 tester（压测客户端）

- 用 `gokubegate.NewClient(ctx, ns, svc, WithHook(...))` 创建 client；
- 按参数执行 `N` 个请求，Hook 记录 `endpoint_picked` / `request_done` 分布与 reuse；
- 输出 JSON 到 stdout：
  ```json
  {"phase":"baseline","requests":500,"byEndpoint":{"pod-xxx":251,"pod-yyy":249},"reusedRatio":0.98,"hostEcho":"svc.ns.svc.cluster.local:8080","errors":0}
  ```
- 退出码：自身错误非 0；**不**做业务断言（断言由 host driver 做）。

## 6. 测试场景与执行流程

driver 的全局流程（每个场景独立或串联，见下）：

1. `TestMain` / suite setup：
   - 若 `GOKUBEGATE_E2E_CLUSTER` 指定已存在集群则复用（节省时间）；否则 `kind create cluster`（`--name gokubegate-e2e`，配置 1 control-plane + 1 worker，内存 2GB/节点）；
   - 构建 `downstream`/`tester` 镜像并 `kind load`；
   - 应用 namespace/RBAC/manifests，等待 downstream Deployment ready；
2. 逐场景执行；
3. `t.Cleanup`：删除 Job/Deployment/namespace；`GOKUBEGATE_E2E_KEEP=1` 保留集群，否则 `kind delete cluster`。

### S1 基础负载均衡

- 前置：downstream 2 副本 Ready；
- 执行：跑 tester Job（500 请求，目标 `http://downstream.gokubegate-e2e.svc.cluster.local:8080/`）；
- 断言（host driver 读取 Job 日志 JSON + 各 Pod `/stats`）：
  - 请求全部成功（errors=0）；
  - 两个 Pod 命中数均在 `[40%, 60%]` 区间（n=500 的经验容差，可配置）；
  - `reusedRatio >= 0.9`（说明连接池生效，不是每次新建连接）；
- 判定标准：以各下游 Pod 的 `/stats` 为权威计数；tester 侧 Hook 分布作交叉验证，允许两者因"请求已在途/连接关闭"存在小偏差。

### S2 Host/路径保真

- 前置：同 S1；
- 执行：tester 请求带自定义 path/query，如 `/api/items?page=1`；downstream 回显 `r.Host` 与 `r.URL`；
- 断言：tester 输出的 `hostEcho == "downstream.gokubegate-e2e.svc.cluster.local:8080"`，且 path/query 原样；另跑一个设置 `req.Host=custom.example.com` 的请求，断言 Host 被尊重（设计文档 8.1）。

### S3 扩容

- 前置：S1 后；downstream 2 副本；
- 步骤：
  1. `kubectl scale deployment downstream --replicas=4`；
  2. 等待 4 个 Pod Ready（`rollout status` / 轮询 EndpointSlice ready 数 == 4）；
  3. 跑 tester Job（500 请求）；
- 断言：4 个 Pod 都收到流量（各 >= 5%，即新 Pod 确实接流）；总请求全部成功。

### S4 缩容 / NotReady

- 前置：4 副本 Ready；
- 场景 A（缩容）：`scale --replicas=2`；等待 Ready == 2；跑 tester；断言：被摘除 Pod 的 `/stats` 不再增长（对比摘除前的计数），存活 Pod 收到全部流量；
- 场景 B（NotReady）：对某个 Pod 调 `/admin/ready?on=false`，等待其 readiness probe 失败、EndpointSlice ready 数 == 3；跑 tester；断言该 Pod 不再收到新请求；随后恢复 ready，断言重新接流；
- 时序说明：摘除/NotReady 后给 2~5s 过渡窗口（EndpointSlice 传播 + informer reconcile），再开始压测。

### S5 RBAC

- 场景 A（最小权限）：默认 tester 用 `rbac.yaml`（endpointslices rw + services get）运行，S1 通过即证明最小权限足够；
- 场景 B（缺权限 fail-fast）：用 `rbac-denied.yaml`（无 endpointslices 权限）跑 tester；断言 tester 启动阶段 `NewClient` 返回错误（日志含 RBAC/list/watch 拒绝），Job 失败退出——对应设计文档 10.1"启动失败 fail fast"。

### S6 SSE 长连接（可选，P3）

- downstream 增加 `GET /stream`：每 500ms 发一条 SSE event，持续 10s，事件内容含自身 Pod 名；
- tester 用 `http.Client{Transport: gate, Timeout: 0}` 建立 1 条流，记录收到的全部事件 Pod 名；
- 断言：一条流内 Pod 名恒定；随后并发 20 条新流，断言 4 个 Pod 都至少被选中 1 次。

### S7 API Server 抖动（可选/高级）

- 思路：`docker pause` 控制面容器 20~30s，期间 informer watch 断开，但 tester 继续请求应全部成功（last-known-good 快照）；
- 注意：pause 整个 control-plane 会影响 kubelet/调度，Pod 可能受影响，结果解读需谨慎；如不稳定则降级为"只验证 watch 断连后恢复"，或推迟到 M3 用更可控的方式（如临时改 informer 端点的网络策略）模拟；
- 标记为实验性，不阻塞 P1/P2。

## 7. 数据采集与断言设计

- **权威计数**：下游各 Pod `/stats`（内存计数）。driver 在压测前后各采集一次，做差值；
- **tester 侧**：Hook 事件统计（picked/request_done/reused），输出 JSON；
- **断言时点**：Job 完成后统一断言（避免边跑边查的时序问题）；阶段切换用 `wait` 工具轮询（默认超时 60s，间隔 500ms）；
- **容差参数**：集中定义在 harness 常量中（`distributionTolerance = 0.20` 等），便于调整；
- **日志**：tester Job 日志 `kubectl logs` 完整拉取，失败时打印上下文便于排查。

## 8. 运行方式与 CI

### 8.1 本地运行

```bash
# 首次：下载 kind 到 .cache
make e2e-bootstrap

# 跑全部 e2e（创建 kind → 测试 → 删除）
make e2e

# 保留集群调试
GOKUBEGATE_E2E_KEEP=1 make e2e

# 只跑指定场景（通过 go test -run）
make e2e ARGS="-run TestE2EScaleUp"

# 清理
make e2e-clean
```

门控：`driver_test.go` 使用 `//go:build e2e` 构建标签，默认 `go test ./...` **不会**执行 e2e；显式 `go test -tags e2e ./test/e2e/...` 才运行。CI 中通过环境变量 `GOKUBEGATE_E2E=1` 双重保护。

### 8.2 全隔离模式（可选增强）

`make e2e-dind`：在 Docker 容器内（`--privileged` + 挂载 docker.sock）运行引导脚本 + e2e，宿主机只需 Docker：

```bash
docker run --rm --privileged \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$(pwd)":/workspace -w /workspace \
  golang:1.26 bash -c "make e2e-bootstrap && make e2e"
```

### 8.3 CI（后续接入）

GitHub Actions 使用官方 `kind` action（`kindest/node` 固定版本），steps：setup-go → checkout → `go build ./...` → `make e2e`。CI 镜像拉取可配置 `GOPROXY` 与 kind 镜像镜像源。

## 9. 风险与对策

| 风险 | 影响 | 对策 |
| --- | --- | --- |
| kind 未安装 / 下载失败 | 无法开始 | 引导脚本 + 固定版本 + sha256 校验；失败给出明确提示 |
| 镜像拉取慢（goproxy.cn 或 kind 基础镜像） | 超时 | 设 `GOPROXY=https://goproxy.cn,direct`；kind 镜像版本固定并预热 |
| 时序不稳定（EndpointSlice 传播、probe 周期） | 断言误报 | 统一 `wait` 轮询 + 过渡窗口；容差可配；失败重试一次 |
| Docker Desktop 资源限制 | 集群起不来 | 单节点配置（1 control-plane + 1 worker）；文档写明最低 4GB |
| 与本地已有集群/kubectl 冲突 | 污染环境 | 全部隔离在 kind 与 `.cache/e2e/`；不写宿主 kubeconfig 默认位置 |
| 场景偶发 flaky | CI 卡顿 | 每个场景独立可 rerun；先跑 P1 场景再扩展 |

## 10. 里程碑

- **P1（骨架 + 基础场景）**：harness（kind/kube/wait）、downstream、tester、Makefile、S1/S2 跑通；
- **P2（生命周期场景）**：S3 扩容、S4 缩容/NotReady、S5 RBAC（含 fail-fast 验证）；
- **P3（可选 + CI）**：S6 SSE、S7 API Server 抖动（实验性）、GitHub Actions 接入、README 完善。

## 11. 待实施前确认项

1. kind 固定版本（建议 v0.27.x）+ 对应 `kindest/node` 镜像 tag；
2. 测试命名空间命名（建议 `gokubegate-e2e`）；
3. 是否接受引导脚本往 `.cache/` 下载 kind（不装系统路径）；
4. e2e 是否纳入常规 CI（当前计划先本地跑，CI 后接）；
5. S7 的模拟方式（pause control-plane vs 其他），若不稳定是否降级为仅"断连恢复"验证。

## 12. 参考资料

- [kind (Kubernetes in Docker)](https://kind.sigs.k8s.io/)
- [kind 官方 GitHub Action](https://github.com/marketplace/actions/kind-kubernetes-in-docker-action)
- [Kubernetes EndpointSlice 概念](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/)
- gokubegate 设计文档：`docs/spec/technical-design.md`（13.2 集成测试、9 生命周期、10 失败处理）
