/*
 * ============================================================================
 * mlc_neb — 聚合管理器（核心引擎）
 * ============================================================================
 *
 * 数据包聚合和压缩的核心引擎。
 *
 * 对每个 NEB 启用的玩家，出站数据包在 *编码前* 被拦截（见 PacketInterceptor），
 * 缓冲原始 NMS Packet<?> 对象。 每 @flushIntervalMs 毫秒刷新一次:
 *   1. 原子地交换出缓冲区列表
 *   2. 对每个子包用其在 Connection 上当前协议的 codec 反射编码出 body 字节
 *      （vanilla 包的 codec 只写 body，不含 PacketID；见客户端 AggregatedEncodePacket）
 *   3. 按客户端 NEB 兼容线格式拼接: [prefix: ns/path varint][varint size][data]
 *   4. 外层: [bool:compressed][varint:rawSize(若压缩)][data]
 *   5. 构造一个 NMS ClientboundCustomPayloadPacket，其 payload 的 type().id()
 *      为 "neb:packet_aggregation_packet" 并携带上述外层字节，
 *      通过 Connection.send(... ) 经正常 PacketEncoder 发出（自动加 PacketID + 长度前缀）。
 *
 * 黑名单中的包由 PacketInterceptor 直接放行（且在此之前触发本类 flush 保持顺序）。
 * 非 NEB 玩家完全不注入拦截器，数据包透明直通。
 *
 * ======== 线程模型 ========
 * buffering 在 Netty I/O 线程（state.bufferPacket）。
 * flush 在主线程（Bukkit 调度器）。
 * 二者通过 PlayerState.swapBuffer() 原子交换协作。
 *
 * ======== NMS 访问 ========
 * 全部经 {@link NmsReflect} 与 {@link NebPayloadFactory} 反射进行。
 * ============================================================================
 */

package org.mlc.mlc_neb.aggregation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.bukkit.entity.Player;
import org.mlc.mlc_neb.MlcNeb;
import org.mlc.mlc_neb.NebConfig;
import org.mlc.mlc_neb.compression.ZstdCompressor;
import org.mlc.mlc_neb.network.NmsReflect;
import org.mlc.mlc_neb.network.NebPayloadFactory;
import org.mlc.mlc_neb.stats.StatsManager;
import org.mlc.mlc_neb.util.PlayerState;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 管理每玩家的数据包缓冲和定期刷新。
 */
public class AggregationManager {

    /** 单玩家缓冲包数硬上限（超过则提前 flush），避免极端积压导致内存膨胀。 */
    private static final int MAX_BUFFERED_PACKETS = 4096;

    /** 单次刷新 rawBuf 不超过 maxPacketSize 的 85%；超过则分批刷新。 */
    private static final float FILL_FRACTION = 0.85f;

    private final MlcNeb plugin;
    private final Logger logger;

    public AggregationManager(MlcNeb plugin) {
        this.plugin = plugin;
        this.logger = plugin.getLogger();
    }

    // ========================================================================
    //  生命周期
    // ========================================================================

    public void shutdown() {
        logger.info("聚合管理器已关闭。");
    }

    // ========================================================================
    //  刷新单个玩家
    // ========================================================================

    /**
     * 刷新指定玩家的缓冲数据包。
     *
     * <p>可由主线程调度器（flushAllPlayers）、黑名单放行前（PacketInterceptor）、
     * 玩家退出/禁用时调用。 主线程语义: 该方法假定调用方持有 PlayerState 的逻辑所有权;
     * 实际线程可以是任意，因 swapBuffer 自带同步。</p>
     */
    public void flushPlayer(PlayerState state) {
        if (state == null) return;
        if (!plugin.getNebConfig().isAggregationEnabled()) {
            // 聚合关闭时丢弃缓冲（保持原放行语义: 关闭聚合时这些包不该被缓冲，正常情况缓冲为空）。
            state.swapBuffer();
            return;
        }
        if (!state.isNebEnabled()) return;

        Player player = state.getPlayer();
        if (player == null || !player.isOnline()) {
            state.swapBuffer();
            return;
        }

        List<Object> packets = state.swapBuffer();
        if (packets == null || packets.isEmpty()) return;

        // 反射拿到 NMS Connection:flush 编码子包需要从中取 ProtocolInfo/codec,
        // 最终也通过它 send 聚合包。 失败则丢弃此批,防止缓冲累积成内存膨胀。
        Object connection = NmsReflect.getConnection(player);
        if (connection == null) {
            // 拿不到连接，丢弃此批以避免无界积压。
            logger.fine("flush " + state.getPlayerName() + ": 拿不到 Connection, 丢弃 "
                    + packets.size() + " 包");
            return;
        }

        try {
            // 两步:
            // (1) buildFrames: 把任意多个缓冲包组装成"外层帧字节"列表,
            //     内部按 max-packet-size 自动分批、并对每批做 Zstd 压缩。
            // (2) sendOneFrame: 把每个外层帧包成一个 NMS CustomPayloadPacket 发送。
            List<byte[]> frames = buildFrames(state, packets, connection);
            for (byte[] frame : frames) {
                sendOneFrame(connection, state, frame);
            }
        } catch (Throwable t) {
            // flush 整体失败不应中断调度器周期(下一 tick 再试)。
            logger.log(Level.WARNING, "刷新玩家 " + state.getPlayerName()
                    + " 的聚合包失败", t);
        }
    }

    // ========================================================================
    //  构造外层帧（可能多批）
    // ========================================================================

    /**
     * 把缓冲的子包逐个编码、按 NEB 线格式拼接到 rawBuf，必要时按 maxPacketSize 分批。
     * 返回每个批次的"外层帧字节"（[bool compressed][varint rawSize(若压)][data]）。
     */
    private List<byte[]> buildFrames(PlayerState state, List<Object> packets,
                                     Object connection) throws Throwable {
        NebConfig cfg = plugin.getNebConfig();
        int maxRaw = (int) (cfg.getMaxPacketSize() * FILL_FRACTION);
        int threshold = cfg.getCompressionThreshold();

        List<byte[]> frames = new ArrayList<>();
        ByteBuf rawBuf = ByteBufAllocator.DEFAULT.buffer(1024);

        try {
            for (Object packet : packets) {
                // 编码单个子包到临时 buf（= body 字节，不含 PacketID）。
                byte[] body = encodeSubPacket(packet, connection);
                if (body == null) {
                    // 无法编码的包跳过（已在编码处告警）。
                    continue;
                }
                String typeId = NmsReflect.getPacketTypeId(packet);
                if (typeId == null) typeId = "minecraft:unknown";

                // prefix + varint(size) + data → piece
                ByteBuf piece = ByteBufAllocator.DEFAULT.buffer();
                try {
                    writeIdentifier(piece, typeId);
                    writeVarInt(piece, body.length);
                    piece.writeBytes(body);

                    // 若加入后超过 maxRaw 且当前帧非空，先封一个帧再开新帧。
                    if (rawBuf.readableBytes() + piece.readableBytes() > maxRaw
                            && rawBuf.readableBytes() > 0) {
                        frames.add(bakeFrame(rawBuf, threshold, state));
                        rawBuf.clear();
                    }
                    rawBuf.writeBytes(piece);
                } finally {
                    piece.release();
                }
            }

            if (rawBuf.readableBytes() > 0) {
                frames.add(bakeFrame(rawBuf, threshold, state));
            }
        } finally {
            rawBuf.release();
        }

        return frames;
    }

    /**
     * 把 rawBuf 的可读字节编码为一个外层帧（含 [bool compressed][varint rawSize][zstd]）。
     * 调用后 rawBuf 被适当处理（不负责释放原始 rawBuf 生命周期）。
     */
    private byte[] bakeFrame(ByteBuf rawBuf, int threshold, PlayerState state)
            throws Throwable {
        int rawSize = rawBuf.readableBytes();
        boolean compress = rawSize >= threshold;
        ByteBuf out = ByteBufAllocator.DEFAULT.buffer(rawSize + 16);

        try {
            out.writeBoolean(compress);
            if (compress) {
                ZstdCompressor compressor = state.getCompressor();
                if (compressor == null) {
                    plugin.getAggregationManager().initCompressor(state);
                    compressor = state.getCompressor();
                }
                byte[] rawBytes = new byte[rawSize];
                rawBuf.getBytes(rawBuf.readerIndex(), rawBytes);

                writeVarInt(out, rawSize);

                byte[] compressed = compressor.compress(rawBytes);
                out.writeBytes(compressed);

                // 记录统计
                StatsManager.recordOutboundRaw(state.getUuid(), rawSize);
                StatsManager.recordOutboundBaked(state.getUuid(),
                        1 + varIntSize(rawSize) + compressed.length);
                state.addOutboundRaw(rawSize);
                state.addOutboundBaked(1 + varIntSize(rawSize) + compressed.length);

                if (plugin.getNebConfig().isDebugLog()) {
                    float ratio = rawSize > 0 ? (100f * compressed.length / rawSize) : 0f;
                    logger.info(String.format(
                            "[%s] 帧刷新: %d → %d bytes (%.1f%%) [Zstd]",
                            state.getPlayerName(), rawSize, compressed.length, ratio));
                }
            } else {
                byte[] rawBytes = new byte[rawSize];
                rawBuf.getBytes(rawBuf.readerIndex(), rawBytes);
                out.writeBytes(rawBytes);

                StatsManager.recordOutboundRaw(state.getUuid(), rawSize);
                StatsManager.recordOutboundBaked(state.getUuid(), 1 + rawSize);
                state.addOutboundRaw(rawSize);
                state.addOutboundBaked(1 + rawSize);

                if (plugin.getNebConfig().isDebugLog()) {
                    logger.info(String.format(
                            "[%s] 帧刷新: %d bytes [未压缩]",
                            state.getPlayerName(), rawSize));
                }
            }

            byte[] frame = new byte[out.readableBytes()];
            out.readBytes(frame);
            return frame;
        } finally {
            out.release();
        }
    }

    // ========================================================================
    //  发送单帧
    // ========================================================================

    private void sendOneFrame(Object connection, PlayerState state, byte[] frame) {
        try {
            Object nebPacket = NebPayloadFactory.createAggregationPacket(frame);
            if (nebPacket == null) {
                // 标准构造路径不可用 → 记录并跳过（避免踢线）。
                logger.fine("发送 NEB 聚合包: createAggregationPacket 返回 null, 跳过本帧 ("
                        + frame.length + " bytes) for " + state.getPlayerName());
                return;
            }
            NmsReflect.sendPacket(connection, nebPacket);
        } catch (Throwable t) {
            logger.log(Level.WARNING, "发送 NEB 聚合包到 "
                    + state.getPlayerName() + " 失败", t);
        }
    }

    // ========================================================================
    //  子包编码（反射: 用 Connection 当前协议的 codec 编码包 body）
    // ========================================================================

    /**
     * 反射用 id-dispatch codec 编码单个子包的 body（不含 PacketID）。
     * 镜像客户端 AggregatedEncodePacket.encodeVanilla 的 IdDispatchCodec 流程。
     *
     * <p>这是 flush 路径中 NMS 反射最复杂、最版本敏感的一环:
     * <pre>
     *   Connection.getOutboundProtocol()  → ProtocolInfo
     *   ProtocolInfo.codec()              → IdDispatchCodec
     *   IdDispatchCodec.typeGetter.apply(packet) → PacketType
     *   IdDispatchCodec.toId.get(type)    → int id
     *   IdDispatchCodec.byId.get(id)      → entry
     *   entry.serializer()                → StreamCodec
     *   StreamCodec.encode(buf, packet)   → 写 body (默认不含 PacketID)
     * </pre>
     * vanilla 包的 codec 只写 body(不含 PacketID),与客户端解码侧
     * {@code codec.decode(data)} 只解 body 严格对称;这保证两端线格式一致。
     * 任一步反射失败返回 null,调用方跳过该子包(在 fine 日志记录)。</p>
     */
    private byte[] encodeSubPacket(Object packet, Object connection) throws Throwable {
        try {
            // 1) Connection → ProtocolInfo. 字段名在不同小版本可能为
            //    outboundProtocol / sendingProtocol 或方法 getOutboundProtocol.
            Object protocolInfo = getProtocolInfo(connection);
            if (protocolInfo == null) return null;

            // 2) ProtocolInfo.codec() → IdDispatchCodec
            Object codec = callNoArg(protocolInfo, "codec");
            if (codec == null) return null;

            // 3) IdDispatchCodec.typeGetter.apply(packet) → PacketType
            //    typeGetter 是 AnyFunction/Predicate 类型的字段; 调用其 apply(Object).
            Object typeGetter = field(codec, "typeGetter");
            Object packetType;
            if (typeGetter != null) {
                packetType = invoke1(typeGetter, "apply", packet);
            } else {
                // 没有 typeGetter 即无法反查 PacketType→id,只能放弃编码此子包。
                return null;
            }

            // 4) toId: PacketType → Integer 协议ID
            java.util.Map<?, ?> toId = (java.util.Map<?, ?>) field(codec, "toId");
            if (toId == null) return null;
            Integer id = (Integer) toId.get(packetType);
            if (id == null) return null;

            // 5) byId: Integer → entry (entry 含该 protocol-id 的具体 serializer)
            java.util.Map<?, ?> byId = (java.util.Map<?, ?>) field(codec, "byId");
            if (byId == null) return null;
            Object entry = byId.get(id);
            if (entry == null) return null;

            // 6) entry.serializer() → StreamCodec
            Object streamCodec = callNoArg(entry, "serializer");
            if (streamCodec == null) return null;

            // 7) StreamCodec.encode(buf, packet) 写 body 到新 ByteBuf
            //    注意:必须配对 release 此 buf (见 finally)。
            ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(256);
            try {
                invoke2(streamCodec, "encode", buf, packet);
                byte[] body = new byte[buf.readableBytes()];
                buf.readBytes(body);
                return body;
            } finally {
                buf.release();
            }
        } catch (Throwable t) {
            // 子包编码失败不应中止整个 flush; 跳过此包并记录供排查。
            String typeId = NmsReflect.getPacketTypeId(packet);
            logger.log(Level.FINE, "无法反射编码子包 " + typeId
                    + " (跳过)", t);
            return null;
        }
    }

    private Object getProtocolInfo(Object connection) throws Throwable {
        // Connection.getSending() 给 PacketFlow; 真正需要 ProtocolInfo。
        // 不同版本 Connection 有不同的 "outboundProtocol" / "getOutboundProtocol" 字段/方法。
        Object p = callNoArg(connection, "getOutboundProtocol");
        if (p == null) p = field(connection, "outboundProtocol");
        if (p == null) p = field(connection, "sendingProtocol");
        return p;
    }

    // ========================================================================
    //  压缩上下文初始化
    // ========================================================================

    /**
     * 为指定玩家初始化 Zstd 压缩上下文。 旧上下文若存在则先关闭。
     *
     * <p>注意: 关闭旧 ctx 与切换存在竞态（flush 中途 reload 时）。
     * 实践中 reload 不重建 compressor（仅 /neb manage 路径会禁/启用，
     * 此时 flush 已先完成）。 见 NebConfig reload 文档。</p>
     */
    public void initCompressor(PlayerState state) {
        ZstdCompressor old = state.getCompressor();
        if (old != null) {
            old.close();
        }

        boolean useContext = plugin.getNebConfig()
                .shouldUseContext(state.getUuid().toString());

        ZstdCompressor compressor = new ZstdCompressor(
                plugin.getNebConfig().getCompressionLevel(),
                plugin.getNebConfig().getContextLevel(),
                useContext
        );

        state.setCompressor(compressor);
    }

    // ========================================================================
    //  VarInt / Identifier 工具（与 Minecraft 协议一致）
    // ========================================================================

    /**
     * 写入 Minecraft Identifier: [varint:nsLen][utf8:ns][varint:pathLen][utf8:path]。
     * 与 AggregatedPacket/客户端 CustomPacketPrefixHelper.write(writeIdentifier 路径) 一致。
     */
    private static void writeIdentifier(ByteBuf buf, String typeId) {
        int colonIdx = typeId.indexOf(':');
        String namespace, path;
        if (colonIdx >= 0) {
            namespace = typeId.substring(0, colonIdx);
            path = typeId.substring(colonIdx + 1);
        } else {
            namespace = "minecraft";
            path = typeId;
        }
        byte[] nsBytes = namespace.getBytes(StandardCharsets.UTF_8);
        byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
        writeVarInt(buf, nsBytes.length);
        buf.writeBytes(nsBytes);
        writeVarInt(buf, pathBytes.length);
        buf.writeBytes(pathBytes);
    }

    static void writeVarInt(ByteBuf buf, int value) {
        while ((value & ~0x7F) != 0) {
            buf.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf.writeByte(value & 0x7F);
    }

    private static int varIntSize(int value) {
        int n = 1;
        while ((value & ~0x7F) != 0) {
            n++;
            value >>>= 7;
        }
        return n;
    }

    // ========================================================================
    //  反射内省工具
    // ========================================================================

    private static Object callNoArg(Object target, String name) {
        if (target == null) return null;
        Class<?> c = target.getClass();
        while (c != null) {
            try {
                java.lang.reflect.Method m = c.getDeclaredMethod(name);
                m.setAccessible(true);
                return m.invoke(target);
            } catch (NoSuchMethodException ignored) {
            } catch (Exception e) {
                return null;
            }
            c = c.getSuperclass();
        }
        return null;
    }

    private static Object invoke1(Object target, String name, Object arg) throws Throwable {
        Class<?> c = target.getClass();
        while (c != null) {
            try {
                java.lang.reflect.Method m = c.getDeclaredMethod(name, Object.class);
                m.setAccessible(true);
                return m.invoke(target, arg);
            } catch (NoSuchMethodException ignored) {
            }
            c = c.getSuperclass();
        }
        // 退而求其次: 按名取单参方法
        for (java.lang.reflect.Method m : target.getClass().getMethods()) {
            if (m.getName().equals(name) && m.getParameterCount() == 1) {
                m.setAccessible(true);
                return m.invoke(target, arg);
            }
        }
        throw new NoSuchMethodException(name + "(Object)");
    }

    private static Object invoke2(Object target, String name, Object a1, Object a2)
            throws Throwable {
        Class<?> c = target.getClass();
        while (c != null) {
            for (java.lang.reflect.Method m : c.getDeclaredMethods()) {
                if (m.getName().equals(name) && m.getParameterCount() == 2) {
                    m.setAccessible(true);
                    return m.invoke(target, a1, a2);
                }
            }
            c = c.getSuperclass();
        }
        throw new NoSuchMethodException(name + "(..,..)");
    }

    private static Object field(Object target, String name) {
        if (target == null) return null;
        Class<?> c = target.getClass();
        while (c != null) {
            try {
                java.lang.reflect.Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(target);
            } catch (NoSuchFieldException ignored) {
            } catch (Exception e) {
                return null;
            }
            c = c.getSuperclass();
        }
        return null;
    }
}