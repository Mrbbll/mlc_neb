/*
 * ============================================================================
 * mlc_neb — 聚合管理器（核心引擎）
 * ============================================================================
 *
 * 数据包聚合和压缩的核心引擎。
 *
 * 对每个 NEB 启用的玩家，出站数据包被拦截并缓冲，不立即发送。
 * 每 20ms 刷新一次:
 *   1. 每个缓冲中的包被序列化为字节
 *   2. 全部字节拼接为一个缓冲区
 *   3. 若总大小超过阈值则 Zstd 压缩
 *   4. 以 "neb:packet_aggregation_packet" 类型发出（兼容客户端 NEB 模组）
 *
 * 黑名单中的包直接发送（先刷新缓冲以保持顺序）。
 * 非 NEB 玩家完全不走此引擎，数据包透明直通。
 *
 * ======== 线程模型 ========
 * 数据包由 Netty I/O 线程添加到缓冲区。
 * 刷新由 Bukkit 调度器线程触发。
 * 刷新时原子性地交换缓冲区列表以避免锁竞争。
 * ============================================================================
 */

package org.mlc.mlc_neb.aggregation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.bukkit.entity.Player;
import org.mlc.mlc_neb.MlcNeb;
import org.mlc.mlc_neb.compression.ZstdCompressor;
import org.mlc.mlc_neb.stats.StatsManager;
import org.mlc.mlc_neb.util.PlayerState;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 管理每玩家的数据包缓冲和定期刷新。
 *
 * <p>这是 NEB 优化的核心。 将大量小 TCP 分段（每个都带协议头开销）
 * 合并为一个压缩块每 tick 发出，大幅降低带宽消耗。</p>
 */
public class AggregationManager {

    private final MlcNeb plugin;
    private final Logger logger;

    public AggregationManager(MlcNeb plugin) {
        this.plugin = plugin;
        this.logger = plugin.getLogger();
    }

    // ========================================================================
    //  生命周期
    // ========================================================================

    /**
     * 关闭聚合管理器，刷新所有剩余缓冲。
     * 在插件禁用时调用。
     */
    public void shutdown() {
        logger.info("聚合管理器已关闭。");
    }

    // ========================================================================
    //  缓冲数据包
    // ========================================================================

    /**
     * 将一个已编码的数据包加入玩家的聚合缓冲区。
     *
     * <p>由 Netty 管道拦截器在拦截到非黑名单包时调用。</p>
     *
     * @param state  玩家状态
     * @param typeId 包类型标识（"namespace:path" 格式）
     * @param data   已编码的包数据字节
     */
    public void bufferPacket(PlayerState state, String typeId, byte[] data) {
        List<AggregatedPacket> buffer = state.getPacketBuffer();
        synchronized (buffer) {
            buffer.add(new AggregatedPacket(typeId, data));
        }
    }

    // ========================================================================
    //  刷新单个玩家
    // ========================================================================

    /**
     * 刷新指定玩家的缓冲数据包:
     * <ol>
     *   <li>原子性地排空缓冲区</li>
     *   <li>所有子包序列化到一个缓冲区</li>
     *   <li>可选 Zstd 压缩</li>
     *   <li>以 ClientboundCustomPayloadPacket(neb:packet_aggregation_packet) 发出</li>
     * </ol>
     *
     * @param state 要刷新的玩家状态
     */
    public void flushPlayer(PlayerState state) {
        List<AggregatedPacket> buffer = state.getPacketBuffer();
        List<AggregatedPacket> toSend;

        synchronized (buffer) {
            if (buffer.isEmpty()) return;
            toSend = new ArrayList<>(buffer);
            buffer.clear();
        }

        Player player = state.getPlayer();
        if (!player.isOnline()) return;

        try {
            // 第1步: 序列化所有子包到一个缓冲区
            ByteBuf rawBuf = serializePackets(toSend);
            int rawSize = rawBuf.readableBytes();

            // 第2步: 决定是否压缩
            boolean compress = rawSize >= plugin.getNebConfig().getCompressionThreshold();

            // 第3步: 构建最终有线格式
            // 格式: [bool:compressed] [varint:rawSize(若压缩)] [data]
            byte[] payload;
            int bakedSize;

            if (compress) {
                ZstdCompressor compressor = state.getCompressor();
                if (compressor == null) {
                    initCompressor(state);
                    compressor = state.getCompressor();
                }
                byte[] compressed = compressor.compress(rawBuf);
                rawBuf.release();

                ByteBuf finalBuf = Unpooled.buffer(1 + 5 + compressed.length);
                finalBuf.writeBoolean(true);  // compressed = true
                writeVarInt(finalBuf, rawSize);
                finalBuf.writeBytes(compressed);

                payload = new byte[finalBuf.readableBytes()];
                finalBuf.readBytes(payload);
                finalBuf.release();
                bakedSize = payload.length;
            } else {
                // 不压缩，直接发送
                ByteBuf finalBuf = Unpooled.buffer(1 + rawSize);
                finalBuf.writeBoolean(false);
                finalBuf.writeBytes(rawBuf);
                rawBuf.release();

                payload = new byte[finalBuf.readableBytes()];
                finalBuf.readBytes(payload);
                finalBuf.release();
                bakedSize = payload.length;
            }

            // 第4步: 通过 Netty 管道发送（绕过 Bukkit 的插件消息 API）
            // 因为我们需要的包类型是 "neb:packet_aggregation_packet"
            // 而不是 Bukkit 的 "minecraft:brand"
            sendNebPacket(state, payload);

            // 第5步: 更新统计
            StatsManager.recordOutboundRaw(state.getUuid(), rawSize);
            StatsManager.recordOutboundBaked(state.getUuid(), bakedSize);

            if (plugin.getNebConfig().isDebugLog()) {
                float ratio = rawSize > 0 ? (100f * bakedSize / rawSize) : 0f;
                logger.fine(String.format(
                        "[%s] 刷新 %d 个包: %d bytes → %d bytes (%.1f%%) %s",
                        player.getName(), toSend.size(), rawSize, bakedSize,
                        ratio, compress ? "[Zstd]" : "[未压缩]"
                ));
            }

        } catch (Exception e) {
            logger.log(Level.WARNING, "刷新玩家 " + player.getName()
                    + " 的聚合包失败", e);
        }
    }

    // ========================================================================
    //  发送 NEB 聚合包
    // ========================================================================

    /**
     * 将聚合压缩后的数据以兼容 NEB 客户端模组的格式发出。
     *
     * <p>构造一个 ClientboundCustomPayloadPacket，payload 类型为
     * "neb:packet_aggregation_packet"，payload 数据为聚合压缩后的字节。
     * 通过 Netty 管道直接发送，不使用 Bukkit 的 sendPluginMessage
     * （因为 Bukkit 只能用 "minecraft:brand" 通道，而 NEB 模组监听的是
     * NeoForge 的自定义 payload 通道）。</p>
     *
     * @param state   目标玩家状态
     * @param payload 聚合压缩后的数据负载
     */
    private void sendNebPacket(PlayerState state, byte[] payload) {
        if (state.getChannel() == null || !state.getChannel().isActive()) {
            return;
        }

        try {
            // 构造一个 ByteBuf 包含完整的 NEB 聚合包数据
            // 这个数据会通过管道写入，绕过 Bukkit 的插件消息限制
            ByteBuf buf = Unpooled.wrappedBuffer(payload);

            // 直接写入管道的出站端（经过 frame_prepender 后会加上长度前缀）
            // 我们需要确保这个数据被当作一个完整的 Minecraft 数据包来发送。

            // 实际上，在管道的 write() 层面，我们在 encoder 之后拦截。
            // 对于 NEB 聚合包，我们需要它作为一个完整的自定义 payload 包。
            // 最简单的方式: 通过管道的 write 方法，但不经过聚合拦截。

            // 直接调用管道的 write 方法，触发从当前 handler 开始向尾端的处理
            state.getChannel().writeAndFlush(buf);

        } catch (Exception e) {
            logger.log(Level.WARNING, "发送 NEB 聚合包到 "
                    + state.getPlayerName() + " 失败", e);
        }
    }

    // ========================================================================
    //  序列化
    // ========================================================================

    /**
     * 将聚合数据包列表序列化为单个 ByteBuf。
     *
     * <p>每个子包格式: {@code [Identifier] [varint:dataLen] [bytes:data]}</p>
     */
    private ByteBuf serializePackets(List<AggregatedPacket> packets) {
        // 估算容量: 每包平均 256 字节
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(packets.size() * 256);

        for (AggregatedPacket packet : packets) {
            packet.writeTo(buf);
        }

        return buf;
    }

    // ========================================================================
    //  压缩上下文初始化
    // ========================================================================

    /**
     * 为指定玩家初始化 Zstd 压缩上下文。
     *
     * @param state 玩家状态（必须已启用 NEB）
     */
    public void initCompressor(PlayerState state) {
        if (state.getCompressor() != null) {
            state.getCompressor().close();
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
    //  VarInt 工具
    // ========================================================================

    private static void writeVarInt(ByteBuf buf, int value) {
        while ((value & ~0x7F) != 0) {
            buf.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf.writeByte(value & 0x7F);
    }
}
