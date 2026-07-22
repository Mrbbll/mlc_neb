/*
 * ============================================================================
 * mlc_neb — 每玩家状态
 * ============================================================================
 *
 * 持有单个玩家的所有连接级状态:
 *   - NEB 启用标志（玩家是否在 NEB 列表中）
 *   - Netty Channel 引用
 *   - 出站数据包缓冲区（原始 NMS Packet<?> 对象，编码前拦截）
 *   - Zstd 压缩上下文
 *   - 流量统计计数器
 *
 * ======== 线程模型 ========
 * buffering 在 Netty I/O 线程执行（PacketInterceptor.write）。
 * flush 在主线程（Bukkit 调度器）执行。
 * 二者通过"交换 + 单线程写"协作:
 *   - Netty 线程只往 activeBuffer 写（同一连接单 I/O 线程，无需并发 list）
 *   - flush 主线程原子地把 activeBuffer 换成新空表，再处理旧表
 *   - bufferedCount 为 volatile，供命令线程无锁读取缓冲规模
 * PlayerState 在玩家加入时创建，退出时销毁。
 * ============================================================================
 */

package org.mlc.mlc_neb.util;

import io.netty.channel.Channel;
import org.bukkit.entity.Player;
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
    /**
     * 当前接收写入的缓冲出站数据包（原始 NMS Packet<?>，编译期以 Object 承载）。
     * 仅由所属连接的单一 Netty I/O 线程写入；flush 由主线程通过 {@link #swapBuffer} 原子交换。
     */
    private List<Object> activeBuffer = new ArrayList<>();

    /** 缓冲包数量（无锁读取用，volatile 可见性）。由 I/O 线程 add 时递增、flush 交换后清零。 */
    private volatile int bufferedCount = 0;

    // ---- 压缩上下文 ----
    /** 每连接独立的 Zstd 压缩上下文（支持上下文复用）。 */
    private ZstdCompressor compressor;

    // ---- 统计 ----
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
    //  缓冲区交换
    // ========================================================================

    /**
     * 将当前活动缓冲区原子地换成一个新空表，返回旧的待处理缓冲区。
     * 由 flush 主线程调用。返回后 I/O 线程的后续写会进入新表。
     *
     * <p>注意：调用方所在线程（主线程）与 I/O 线程对 activeBuffer 字段的可见性
     * 通过 synchronized 保证；为避免对 I/O 线程持有过久，使用简单锁。
     * 同一连接的 I/O 线程是单线程，竞争极低。</p>
     */
    public synchronized List<Object> swapBuffer() {
        List<Object> old = activeBuffer;
        activeBuffer = new ArrayList<>();
        bufferedCount = 0;
        return old;
    }

    /**
     * 向当前活动缓冲区追加一个已拦截的 NMS Packet（Object 承载）。
     * 由 Netty I/O 线程调用。
     */
    public synchronized void bufferPacket(Object packet) {
        activeBuffer.add(packet);
        bufferedCount = activeBuffer.size();
    }

    /** 当前缓冲包数量的无锁快照（命令显示用，允许近似）。 */
    public int getBufferedCount() {
        return bufferedCount;
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
        if (channel != null) {
            try {
                if (channel.pipeline().get("neb_interceptor") != null) {
                    channel.pipeline().remove("neb_interceptor");
                }
            } catch (Exception ignored) {
                // 管道可能已经关闭
            }
        }

        synchronized (this) {
            activeBuffer.clear();
            bufferedCount = 0;
        }
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

    public ZstdCompressor getCompressor() { return compressor; }
    public void setCompressor(ZstdCompressor compressor) { this.compressor = compressor; }

    // ---- 统计累加器 ----
    public void addOutboundRaw(long bytes) { this.outboundBytesRaw += bytes; }
    public void addOutboundBaked(long bytes) { this.outboundBytesBaked += bytes; }

    public long getOutboundBytesRaw() { return outboundBytesRaw; }
    public long getOutboundBytesBaked() { return outboundBytesBaked; }

    /** 重置所有统计计数器。 */
    public void resetStats() {
        outboundBytesRaw = 0;
        outboundBytesBaked = 0;
    }
}