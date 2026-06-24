# routerload — 真实 router 并发压测器

复刻大客户的压测方法,打真实 router(默认 `api.gonkascan.com/v1`),并补上客户测不到的关键指标:**in-flight 并发**。

这是 open-loop 压测:按"启动速率"(request starts/sec)发请求,**不等上一个返回就发下一个**——只有这样才能复现 in-flight 累积(`in-flight = 到达率 × 延迟`),也就是客户撞到的"并发 429 悬崖"。

它和进程内的协调层测试([../../user/escrow_concurrency_test.go](../../user/escrow_concurrency_test.go))互补:那个测 devshard 协调层机制(隔离 GPU/网关),这个测端到端真实容量。

## 用法

```bash
export ROUTER_API_KEY=sk-...        # Bearer key,绝不硬编码/打印/写文件

cd devshard
go run ./cmd/routerload \
  -url https://api.gonkascan.com/v1/chat/completions \
  -model MiniMaxAI/MiniMax-M2.7 \
  -rps 5,10,15,20,25 \
  -dur 60s \
  -max-tokens 4096 \
  -prompt-tokens 2000 \
  -timeout 90s \
  -label 1escrow \
  -detail detail-1escrow.csv
```

跑完得到:
- 控制台实时表 + `routerload-<label>.csv` 汇总,列:
  `rps,sent,200,429,timeout,other,p50ms,p95ms,p99ms,maxms,max_inflight,first429@s`
- 第一个 429 的**原始 body**(打到 stderr,用来确认是不是 `too many concurrent requests`)
- 可选 `-detail` 每请求 CSV(含 request_id,可拿去 upstream 日志按 id 查 trace)

## 怎么读结果(找瓶颈)

- **`max_inflight` 是核心**:看 429 在哪个 in-flight 水平开始出现,那就是并发上限。
- **`first429@s`**:429 是一上来就有(稳态超限)还是跑了一会才出现(累积型,对应"撑一会→崩")。
- RPS 能跑满全程且 `429+timeout` < ~1% 的最高档 = 可持续 RPS。

## 1 vs 2 escrow 对比 —— 用自建 gateway,不动线上

escrow 数是 **gateway(=router)的配置**,不是客户端开关。最干净的测法是
**自己起一个 gateway**(和线上同款 `devshardctl` 镜像),在上面切 1↔2 escrow,
对线上零影响。完整步骤见 [testgateway/RUNBOOK.md](testgateway/RUNBOOK.md):

1. 起 gateway(`testgateway/docker-compose.yml` + `config.devshard.env`)
2. admin API 建 1 个 escrow → 跑 `-label 1escrow`
3. admin API 再建 1 个 escrow(pool 模式自动负载均衡)→ 跑 `-label 2escrow`
4. diff 两份 CSV:同等 RPS 下 `max_inflight` 上限、429 率、吞吐变化

> 真凶大概率是 gateway 的 `GATEWAY_MAX_CONCURRENT_REQUESTS`(默认示例 512,
> 配合 `DEVSHARD_CAPACITY_AWARE_LIMITS=on` 动态压低)——这就是
> `too many concurrent requests` 的来源。测试前把这些参数调成和线上一致。
>
> 前置门槛:创建者地址要在链上白名单(走治理,不能自己加)+ 真 ngonka 押金。
> 详见 RUNBOOK §0。

## 两个对照链路(强烈建议都跑)

| `-url` | 测什么 | 用途 |
|---|---|---|
| `https://api.gonkascan.com/v1/chat/completions` | 端到端(含 new-api 网关) | 客户视角 |
| 直连 dapi/router 的 OpenAI 端点 | 纯 gonka+devshard | 排除网关干扰 |

**若端到端撞 429 而直连不撞 → 瓶颈在 new-api 网关配置,不是 escrow/devshard。**

## ⚠️ 成本警告

发的是真实计费推理请求。`-rps 25 -dur 60s` 单这一档就 ~1500 个请求。**从小往大跑**。Ctrl-C 干净退出并打印已完成部分。
