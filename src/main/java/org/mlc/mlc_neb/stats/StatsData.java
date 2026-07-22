/*
 * ============================================================================
 * mlc_neb — 统计数据记录
 * ============================================================================
 *
 * 每玩家的统计数据容器。持有:
 *   - 累计字节计数（原始和压缩后，入站和出站）
 *   - 用于传输速率计算的滑动速度窗口
 *
 * 所有字段使用 AtomicLong 以实现线程安全的累加。
 * ============================================================================
 */

package org.mlc.mlc_neb.stats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 持有每玩家的网络统计数据。
 *
 * <p>所有计数器为 {@link AtomicLong}，支持从多线程无锁更新
 * （Netty I/O 线程 + Bukkit 调度器线程）。</p>
 */
public class StatsData {

    // ---- 累计字节总计 ----

    /** 出站原始字节（压缩前/实际包数据）。 */
    public final AtomicLong outboundBytesRaw = new AtomicLong();

    /** 出站实际字节（压缩后/线字节）。 */
    public final AtomicLong outboundBytesBaked = new AtomicLong();

    // ---- 滑动速度窗口 ----

    /** 出站原始传输速度（字节/秒，2秒窗口）。 */
    public final TimeWindowCounter outboundSpeedRaw = new TimeWindowCounter(2000);

    /** 出站实际传输速度（字节/秒，2秒窗口）。 */
    public final TimeWindowCounter outboundSpeedBaked = new TimeWindowCounter(2000);

    // ========================================================================
    //  工具
    // ========================================================================

    /** 重置该玩家的所有计数器。 */
    public void reset() {
        outboundBytesRaw.set(0);
        outboundBytesBaked.set(0);
        outboundSpeedRaw.reset();
        outboundSpeedBaked.reset();
    }

    /**
     * 返回出站压缩率。越小越好（如 15.5 表示节省了 84.5% 带宽）。
     */
    public double getOutboundRatio() {
        long raw = outboundBytesRaw.get();
        long baked = outboundBytesBaked.get();
        if (raw == 0) return 100.0;
        return 100.0 * baked / raw;
    }
}
