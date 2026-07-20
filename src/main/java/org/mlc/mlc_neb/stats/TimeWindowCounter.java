/*
 * ============================================================================
 * mlc_neb — 时间窗口计数器
 * ============================================================================
 *
 * 为计算传输速度（字节/秒）而设计的简单滑动时间窗口计数器。
 * 记录带时间戳的值，在配置的时间窗口上计算平均值。
 *
 * 示例:
 *   TimeWindowCounter counter = new TimeWindowCounter(2000); // 2 秒窗口
 *   counter.put(1500);  // 1500 字节
 *   counter.put(2000);  // 2000 字节
 *   double speed = counter.averageIn1s(); // (1500+2000) / 2 = 1750 B/s
 *
 * 移植自原始 NEB 的 TimeCounter (cn.ussshenzhou.notenoughbandwidth.util)。
 * ============================================================================
 */

package org.mlc.mlc_neb.stats;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 在滑动时间窗口内累积字节计数并计算平均传输速度。
 *
 * <p>线程安全: 所有公共方法使用并发数据结构。</p>
 */
public class TimeWindowCounter {

    /** 时间窗口大小（毫秒）。 */
    private final long windowSizeMs;

    /** 时间桶 → 字节计数 条目。 */
    private final ConcurrentMap<Long, AtomicLong> entries;

    // ========================================================================
    //  构造
    // ========================================================================

    /** 使用默认 2 秒窗口创建。 */
    public TimeWindowCounter() {
        this(2000);
    }

    /**
     * 创建时间窗口计数器。
     *
     * @param windowSizeMs 滑动窗口大小（毫秒）。
     *                     典型值: 1000（1秒）或 2000（2秒）。
     *                     更大的窗口给出更平滑的速度读数但响应更慢。
     */
    public TimeWindowCounter(long windowSizeMs) {
        this.windowSizeMs = windowSizeMs;
        this.entries = new ConcurrentHashMap<>();
    }

    // ========================================================================
    //  写入
    // ========================================================================

    /**
     * 在当前时间记录一个字节计数。
     *
     * @param bytes 传输的字节数
     */
    public void put(int bytes) {
        long now = System.currentTimeMillis();
        long bucket = now / 50; // 50ms 桶（约 1 tick）
        prune(now);

        entries.computeIfAbsent(bucket, k -> new AtomicLong())
                .addAndGet(bytes);
    }

    // ========================================================================
    //  读取
    // ========================================================================

    /**
     * 返回配置时间窗口上的平均传输速率（字节/秒）。
     *
     * @return 平均每秒字节数，无数据时返回 0
     */
    public double averageIn1s() {
        long now = System.currentTimeMillis();
        prune(now);

        long total = entries.values().stream()
                .mapToLong(AtomicLong::get)
                .sum();

        return total / ((double) windowSizeMs / 1000.0);
    }

    /** 返回当前窗口内记录的总字节数。 */
    public long totalInWindow() {
        prune(System.currentTimeMillis());
        return entries.values().stream()
                .mapToLong(AtomicLong::get)
                .sum();
    }

    // ========================================================================
    //  维护
    // ========================================================================

    /** 移除超过窗口期的条目。 */
    private void prune(long now) {
        long cutoff = now - windowSizeMs;
        long cutoffBucket = cutoff / 50;
        entries.keySet().removeIf(bucket -> bucket < cutoffBucket);
    }

    /** 清除所有记录数据。 */
    public void reset() {
        entries.clear();
    }
}
