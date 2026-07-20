/*
 * ============================================================================
 * mlc_neb — 每玩家状态
 * ============================================================================
 *
 * 持有单个玩家的所有连接级状态:
 *   - NEB 启用标志（玩家是否在 NEB 列表中）
 *   - Netty Channel 引用
 *   - 出站数据包缓冲区
 *   - Zstd 压缩上下文
 *   - 流量统计计数器
 *
 * PlayerState 在玩家加入时创建，退出时销毁。
 * ============================================================================
 */

package org.mlc.mlc_neb.util;

import io.netty.channel.Channel;
import org.bukkit.entity.Player;
import org.mlc.mlc_neb.aggregation.AggregatedPacket;
import org.mlc.mlc_neb.compression.ZstdCompressor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 每玩家状态容器。
 *
 * <p>每位在线玩家有唯一一个 PlayerState 实例。
 * {@link #nebEnabled} 标志决定走聚合压缩路径还是原版路径。</p>
 */
public class PlayerState {

    // ---- 身份信息 ----
    private final UUID uuid;
    private final String playerName;
    private final Player player;

    // ---- NEB 状态 ----
    /** 是否启用 NEB 优化（聚合+压缩）。由握手、配置列表或命令控制。 */
    private volatile boolean nebEnabled = false;

    /** 握手是否已完成（已收到 ack 或已超时回退）。 */
    private volatile boolean handshakeComplete = false;

    // ---- Netty 连接 ----
    /** 该玩家连接的 Netty Channel。用于注入数据包拦截器。 */
    private Channel channel;

    // ---- 聚合缓冲区 ----
    /** 等待刷新的缓冲出站数据包。访问由 AggregationManager 管理同步。 */
    private final List<AggregatedPacket> packetBuffer = new ArrayList<>();

    // ---- 压缩上下文 ----
    /** 每连接独立的 Zstd 压缩上下文（支持上下文复用）。 */
    private ZstdCompressor compressor;

    // ---- 统计 ----
    private long inboundBytesRaw = 0;
    private long inboundBytesBaked = 0;
    private long outboundBytesRaw = 0;
    private long outboundBytesBaked = 0;

    // ========================================================================
    //  构造
    // ========================================================================

    public PlayerState(Player player) {
        this.uuid = player.getUniqueId();
        this.playerName = player.getName();
        this.player = player;
    }

    // ========================================================================
    //  清理
    // ========================================================================

    /**
     * 释放该玩家关联的所有资源。
     * 在玩家退出或 NEB 被禁用时调用。
     */
    public void cleanup() {
        // 关闭 Zstd 上下文
        if (compressor != null) {
            compressor.close();
            compressor = null;
        }

        // 从 Netty 管道移除拦截器
        if (channel != null && channel.isActive()) {
            try {
                if (channel.pipeline().get("neb_interceptor") != null) {
                    channel.pipeline().remove("neb_interceptor");
                }
            } catch (Exception ignored) {
                // 管道可能已经关闭
            }
        }

        packetBuffer.clear();
        channel = null;
    }

    // ========================================================================
    //  Getter / Setter
    // ========================================================================

    public UUID getUuid() { return uuid; }
    public String getPlayerName() { return playerName; }
    public Player getPlayer() { return player; }

    public boolean isNebEnabled() { return nebEnabled; }
    public void setNebEnabled(boolean nebEnabled) { this.nebEnabled = nebEnabled; }

    public boolean isHandshakeComplete() { return handshakeComplete; }
    public void setHandshakeComplete(boolean complete) { this.handshakeComplete = complete; }

    public Channel getChannel() { return channel; }
    public void setChannel(Channel channel) { this.channel = channel; }

    public List<AggregatedPacket> getPacketBuffer() { return packetBuffer; }

    public ZstdCompressor getCompressor() { return compressor; }
    public void setCompressor(ZstdCompressor compressor) { this.compressor = compressor; }

    // ---- 统计累加器 ----
    public void addInboundRaw(long bytes) { this.inboundBytesRaw += bytes; }
    public void addInboundBaked(long bytes) { this.inboundBytesBaked += bytes; }
    public void addOutboundRaw(long bytes) { this.outboundBytesRaw += bytes; }
    public void addOutboundBaked(long bytes) { this.outboundBytesBaked += bytes; }

    public long getInboundBytesRaw() { return inboundBytesRaw; }
    public long getInboundBytesBaked() { return inboundBytesBaked; }
    public long getOutboundBytesRaw() { return outboundBytesRaw; }
    public long getOutboundBytesBaked() { return outboundBytesBaked; }

    /** 重置所有统计计数器。 */
    public void resetStats() {
        inboundBytesRaw = 0;
        inboundBytesBaked = 0;
        outboundBytesRaw = 0;
        outboundBytesBaked = 0;
    }
}
