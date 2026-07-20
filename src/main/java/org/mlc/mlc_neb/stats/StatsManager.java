/*
 * ============================================================================
 * mlc_neb — 统计管理器
 * ============================================================================
 *
 * 追踪网络带宽统计:
 *   - 入站/出站字节计数（原始 vs 压缩后）
 *   - 当前传输速度（滑动时间窗口平均值）
 *   - 每玩家和全局统计
 *
 * "原始" = 压缩前的实际数据包数据
 * "压缩后" = 压缩后的实际线字节
 *
 * 显示给用户的"压缩率": 压缩后 / 原始 × 100
 * （越低越好 — 15% 表示节省 85% 带宽）
 *
 * 这些统计通过以下方式展示:
 *   - /neb stats 命令（服务端命令，所有人可用）
 *   - NEB 客户端模组的 Alt+N 覆盖层（仅 NEB 玩家）
 * ============================================================================
 */

package org.mlc.mlc_neb.stats;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * NEB 的中央统计追踪。
 *
 * <p>所有方法为静态。统计按玩家和全局追踪。</p>
 */
public class StatsManager {

    // ---- 每玩家统计 ----
    private static final Map<UUID, StatsData> playerStats = new ConcurrentHashMap<>();

    // ---- 全局统计 ----
    private static final AtomicLong globalInboundRaw = new AtomicLong();
    private static final AtomicLong globalInboundBaked = new AtomicLong();
    private static final AtomicLong globalOutboundRaw = new AtomicLong();
    private static final AtomicLong globalOutboundBaked = new AtomicLong();

    private static final TimeWindowCounter globalInSpeedRaw = new TimeWindowCounter(2000);
    private static final TimeWindowCounter globalInSpeedBaked = new TimeWindowCounter(2000);
    private static final TimeWindowCounter globalOutSpeedRaw = new TimeWindowCounter(2000);
    private static final TimeWindowCounter globalOutSpeedBaked = new TimeWindowCounter(2000);

    private static volatile boolean initialized = false;

    // ========================================================================
    //  生命周期
    // ========================================================================

    /** 初始化统计管理器。幂等。 */
    public static void init() {
        if (initialized) return;
        initialized = true;
        playerStats.clear();
    }

    /** 注册玩家以进行统计追踪。玩家加入时自动调用。 */
    public static void registerPlayer(UUID uuid) {
        playerStats.computeIfAbsent(uuid, k -> new StatsData());
    }

    /** 移除玩家统计。玩家退出时调用。 */
    public static void removePlayer(UUID uuid) {
        playerStats.remove(uuid);
    }

    // ========================================================================
    //  记录
    // ========================================================================

    /** 记录出站（服务端→客户端）原始字节数。 */
    public static void recordOutboundRaw(UUID uuid, long bytes) {
        StatsData data = playerStats.get(uuid);
        if (data != null) {
            data.outboundBytesRaw.addAndGet(bytes);
            data.outboundSpeedRaw.put((int) bytes);
        }
        globalOutboundRaw.addAndGet(bytes);
        globalOutSpeedRaw.put((int) bytes);
    }

    /** 记录出站（服务端→客户端）压缩后字节数。 */
    public static void recordOutboundBaked(UUID uuid, long bytes) {
        StatsData data = playerStats.get(uuid);
        if (data != null) {
            data.outboundBytesBaked.addAndGet(bytes);
            data.outboundSpeedBaked.put((int) bytes);
        }
        globalOutboundBaked.addAndGet(bytes);
        globalOutSpeedBaked.put((int) bytes);
    }

    /** 记录入站（客户端→服务端）原始字节数。 */
    public static void recordInboundRaw(UUID uuid, long bytes) {
        StatsData data = playerStats.get(uuid);
        if (data != null) {
            data.inboundBytesRaw.addAndGet(bytes);
            data.inboundSpeedRaw.put((int) bytes);
        }
        globalInboundRaw.addAndGet(bytes);
        globalInSpeedRaw.put((int) bytes);
    }

    /** 记录入站（客户端→服务端）压缩后字节数。 */
    public static void recordInboundBaked(UUID uuid, long bytes) {
        StatsData data = playerStats.get(uuid);
        if (data != null) {
            data.inboundBytesBaked.addAndGet(bytes);
            data.inboundSpeedBaked.put((int) bytes);
        }
        globalInboundBaked.addAndGet(bytes);
        globalInSpeedBaked.put((int) bytes);
    }

    // ========================================================================
    //  查询
    // ========================================================================

    public static StatsData getPlayerStats(UUID uuid) {
        return playerStats.get(uuid);
    }

    public static Map<UUID, StatsData> getAllPlayerStats() {
        return Map.copyOf(playerStats);
    }

    // ---- 全局总计 ----
    public static long getGlobalInboundRaw() { return globalInboundRaw.get(); }
    public static long getGlobalInboundBaked() { return globalInboundBaked.get(); }
    public static long getGlobalOutboundRaw() { return globalOutboundRaw.get(); }
    public static long getGlobalOutboundBaked() { return globalOutboundBaked.get(); }

    // ---- 全局速度（字节/秒）----
    public static double getGlobalInboundSpeedRaw() { return globalInSpeedRaw.averageIn1s(); }
    public static double getGlobalInboundSpeedBaked() { return globalInSpeedBaked.averageIn1s(); }
    public static double getGlobalOutboundSpeedRaw() { return globalOutSpeedRaw.averageIn1s(); }
    public static double getGlobalOutboundSpeedBaked() { return globalOutSpeedBaked.averageIn1s(); }

    // ---- 压缩率 ----

    /** 返回全局出站压缩率（百分比）。100% = 无压缩，15% = 节省 85% 带宽。 */
    public static double getGlobalOutboundRatio() {
        long raw = globalOutboundRaw.get();
        long baked = globalOutboundBaked.get();
        if (raw == 0) return 100.0;
        return 100.0 * baked / raw;
    }

    /** 返回全局入站压缩率。 */
    public static double getGlobalInboundRatio() {
        long raw = globalInboundRaw.get();
        long baked = globalInboundBaked.get();
        if (raw == 0) return 100.0;
        return 100.0 * baked / raw;
    }

    // ========================================================================
    //  重置
    // ========================================================================

    /** 重置所有统计计数器。 */
    public static void resetAll() {
        playerStats.values().forEach(StatsData::reset);
        globalInboundRaw.set(0);
        globalInboundBaked.set(0);
        globalOutboundRaw.set(0);
        globalOutboundBaked.set(0);
    }
}
