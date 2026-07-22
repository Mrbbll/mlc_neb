# mlc_neb — Not Enough Bandwidth (NEB) Paper 移植版

将 NeoForge 客户端模组 **"Not Enough Bandwidth" (NEB)** (`https://github.com/...USS_Shenzhou/NotEnoughBandwidth`,
本仓库同级目录 `../NotEnoughBandwidth`) 的带宽优化技术移植到 Paper 服务端。

> 本文档面向维护者。 普通用户请直接看下方的 [功能](#功能)、[配置](#配置) 与 [用法](#用法) 三节。

---

## 功能

| 功能 | 说明 | 影响范围 |
|------|------|----------|
| **数据包聚合 + Zstd 压缩** | 每 20ms 将 NEB 玩家的全部出站包缓冲→合并→Zstd 压缩→作为一个 `neb:packet_aggregation_packet` 自定义 payload 发出。 客户端 NEB 模组解压→拆分→注入管道。 | 仅对"已启用 NEB"的玩家生效(见下) |
| **流量统计** | `/neb stats` 显示每玩家/全局的**出站**压缩率与速率。 | 全局可查 |
| ~~延迟区块缓存 (DCC)~~ | 已移除。 纯 Paper 插件无法拦截 `ChunkMap.updateChunkTracking`(需 Mixin),原实现只是空跑内存表,无实际效果。 | — |

### NEB 的"已启用"判定

每位玩家按如下优先级决定是否走聚合:

1. `force-enable-all: true` → 全部启用(跳过握手)
2. 玩家 UUID 在 `neb-enabled-players` 列表 → 启用
3. 握手探测(`neb:handshake` plugin channel):服务端发 `NEB|1.0|hello`,超时 3s 内收到 `NEB|1.0|ack` → 自动启用
4. 超时 → 回退到 (1)(2) 判定;都不成立则走原版路径(透明兼容)

> ⚠️ 当前客户端 NotEnoughBandwidth 模组**未实现握手响应**,故自动检测实际总是走超时回退。
> 未来客户端加握手支持后会自动生效。 现阶段请用 `force-enable-all` 或 `neb-enabled-players`。

非启用玩家**完全不受影响**:拦截器根本不注入其 Netty 管道,所有包照常流通。

---

## 工作原理(给维护者)

### 总体数据流

```
                    ┌─ 非NEB玩家 ─→ 不注入拦截器,原版直通
Minecraft发包        │
   ↓                │
Connection.send ─→ Channel.write(Picket<?>)
   ↓                │
[neb_interceptor] ←─┘ (注入点:packet_encoder 之前)
   ↓ 拿到原始 Packet<?> + packet.type().id()
   ├─ 命中系统黑名单/兼容黑名单 → 先flush本玩家缓冲 → 直接放行(保持时序)
   ├─ BundlePacket → 拆包逐个递归
   └─ 其它 → 存入 PlayerState.activeBuffer + promise.setSuccess(不复位下传)

主线程调度器 每 20ms:
  flushAllPlayers → 对每玩家 flushPlayer:
    swapBuffer 拿到待发列表
    对每个 Packet<?> 用真实 ProtocolInfo 的 IdDispatchCodec 反射编码出 body
    按 NEB 线格式拼接 → Zstd 压缩 → 外层帧
    NebPayloadFactory 构造 ClientboundCustomPayloadPacket(neb:packet_aggregation_packet)
    Connection.send(...) → 正常 PacketEncoder → frame_prepender → 线
```

### 关键设计决策

1. **拦截点 = PacketEncoder 之前**,而非之后。 编码后只剩裸字节无法拿包类型(原作者是"unknown"硬编码,导致黑名单失效、客户端解包失败)。 见 `PacketInterceptor` 顶部注释。
2. **全反射 NMS,不引入 paperweight-userdev**,保持 `build.gradle` 简单。 所有 `net.minecraft.*` 与 `craftbukkit` 访问集中在 `NmsReflect`;构造 NEB payload 在 `NebPayloadFactory`。 代价是版本敏感性,详见 [风险与限制](#风险与限制)。
3. **线程模型**:buffering 在 Netty I/O 线程,flush 在主线程,通过 `PlayerState.swapBuffer()` 原子交换协作。
4. **子包编码**:vanilla 包用其 `IdDispatchCodec` 里 `byId[id].serializer().encode(buf, packet)`,只写 body(不含 PacketID),与客户端解码侧 `codec.decode(data)` 对称。 见 `AggregationManager.encodeSubPacket`。

### NEB 线格式(与客户端 `PacketAggregationPacket` 兼容)

```
外层帧:
  [bool:compressed]
  [varint:rawSize]          仅 compressed=true 时存在
  [data]

data(未压缩或解压后)是一串子包:
  [prefix][varint:dataLen][data]
  prefix = [varint:nsLen][utf8:ns][varint:pathLen][utf8:path]   (即 writeIdentifier)
```

幅压缩用 Zstd,magicless + 可选上下文复用,与客户端 `ZstdHelper.Context` 对齐。

---

## 构建

```bash
# 需要 JDK 21
./gradlew build
# 产物: build/libs/mlc_neb-1.0-SNAPSHOT.jar  (zstd-jni 已 relocate 进 jar)
```

本地试跑(1.21):

```bash
./gradlew runServer
```

> `run-paper` 需要联网下载 Paper server jar。

依赖:
- `io.papermc.paper:paper-api:1.21.11-R0.1-SNAPSHOT` (compileOnly,运行时由服务端提供)
- `io.netty:netty-all:4.1.118.Final` (compileOnly)
- `com.github.luben:zstd-jni:1.5.6-9` (implementation,shadowJar relocate 到 `org.mlc.mlc_neb.lib.zstd`)

---

## 配置

`plugins/mlc_neb/config.yml`(首启自动生成,`/neb reload` 热重载):

```yaml
force-enable-all: false        # 强制所有玩家启用
neb-enabled-players: []        # 或 ["uuid-1", "uuid-2"]

aggregation:
  enabled: true
  flush-interval-ms: 20        # 10-100
  compression-threshold: 32    # 原始字节小于此值则不压缩
  compression-level: 3         # 1-22
  context-level: 23            # 21-25, 21=2MB .. 25=32MB
  max-packet-size: "4MB"       # 2MB-64MB, 单帧上限(超过自动分批)

compatibility:
  enabled: false               # 兼容模式:黑名单中包绕过聚合
  blacklist: [...]             # 默认含 command_suggestions/commands/chat_command 等

context-exclusion:
  player-uuids: []             # 这些玩家不启用 Zstd 上下文复用(如回放Mod)

debug:
  log: false                   # 详细刷新日志,繁忙服务器慎开
```

> **注意**:`compression-level` / `context-level` 的变更对**已在 NEB 中的在线玩家不生效**,
> 需对其 `/neb disable` 再 `/neb enable` 才重建 Zstd 上下文。 reload 提示也会说明。

---

## 用法

```
/neb              # 同 /neb stats
/neb stats        # 全局出站统计
/neb stats <玩家>  # 该玩家统计
/neb list         # 当前启用 NEB 的玩家
/neb enable <玩家名|UUID>
/neb disable <玩家名|UUID>
/neb reload
/neb reset        # 重置统计
/neb version
```

权限(均默认 op):`mlc_neb.command` / `mlc_neb.stats` / `mlc_neb.reload` / `mlc_neb.manage`

---

## 包结构

```
org.mlc.mlc_neb
├── MlcNeb              插件主类:生命周期、事件、握手、启用判定
├── NebConfig           配置读写 + 系统黑名单
├── aggregation/
│   └── AggregationManager   核心引擎:缓冲/刷新/编码/发送  (AggregatedPacket 已移除)
├── network/
│   ├── NmsReflect           所有 NMS/craftbukkit 反射(集中、抗版本)
│   ├── NebPayloadFactory    构造 neb 聚合 payload + 包进 CustomPayloadPacket
│   └── PacketInterceptor    Netty handler:编码前拦截 Packet<?>
├── compression/ZstdCompressor   每连接 Zstd 上下文
├── handshake/HandshakeManager   plugin-channel 握手探测
├── stats/
│   ├── StatsManager    全局/每玩家出站统计
│   ├── StatsData       每玩家计数容器
│   └── TimeWindowCounter  滑动窗口速率
└── command/NebCommand      /neb 命令
```

---

## 风险与限制(必读)

本次重构修复了原文"聚合链路完全失效"的问题,但有几处风险需在真实 1.21.11 服务端实测确认:

1. **发送路径(最高风险)**:标准 `Connection.send(ClientboundCustomPayloadPacket(neb payload))` 在服务端 `NetworkRegistry` 中**未注册** `neb:` payload,可能被拒。
   - 当前 `sendOneFrame` 对失败是 catch+跳过,**此路径不通时 NEB 玩家的包会丢失**(服务器不崩,但客户端表现为卡/掉事件)。
   - 实测若发现丢包/踢线,需补一条"绕过注册、裸字节+PacketID 写入 channel"的发送路径;告知维护者可据此补。

2. **NMS 反射敏感性**:字段名 `connection` / `channel` / `Packet.type()` / `PacketType.id()` / `IdDispatchCodec` 的 `typeGetter`/`toId`/`byId`/`entry.serializer()` 均按 mojmap(1.21.11)假设。 跨小版本升级若其中任一改名,`NmsReflect.init()` 会落地为 `available=false`,**自动降级为原版直通**(不踢线),但聚合失效。 启动日志会打印确切断点。

3. **无法端到端测试**:本仓库维护环境无法联网下载 Paper server jar,故运行期行为未在沙箱内验证。 请由维护者在真实服务端跑 [验证清单](#验证清单)。

### 验证清单

1. 启动看日志 `NMS 反射初始化成功 (cb=..., Connection=..., Packet=...)`。 失败则把堆栈贴回。
2. `force-enable-all: true` + 带 NEB 客户端模组的玩家连入:不被踢、`Alt+N` 覆盖层压缩率非 100%、移动/区块正常。
3. 无 NEB 模组的原版客户端在 `force-enable-all: false` 下正常游戏(透明直通)。
4. `/neb reload`、`enable/disable` 不抛异常、不踢玩家。
5. 玩家反复进出 20 次后 netty direct 内存稳定(无 ByteBuf 泄漏)。

---

## 致谢

原始 NeoForge 模组 **Not Enough Bandwidth** by USS_Shenzhou。 本 Paper 移植仅复用其协议格式与压缩思路,实现完全独立。