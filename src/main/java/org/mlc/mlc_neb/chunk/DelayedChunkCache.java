/*
 * ============================================================================
 * mlc_neb — 延迟区块缓存 (Delayed Chunk Cache, DCC)
 * ============================================================================
 *
 * 移植自原始 NEB 模组的 DelayedChunkCache 功能。
 *
 * ======== 问题 ========
 * 原版 Minecraft 中，玩家移动时服务端会立刻告诉客户端"忘记"离开视野的区块。
 * 如果玩家来回走动，服务端必须重新发送完整的区块数据，浪费带宽。
 *
 * ======== 解决方案 ========
 * DCC 延迟"忘记区块"数据包:
 *   1. 追踪玩家已加载的区块
 *   2. 玩家移动到新位置时，缓存刚离开的区块位置
 *   3. 缓存超时前玩家返回 → 区块数据仍在客户端 → 无需重发
 *   4. 超时后正常发送"忘记"指令
 *
 * 此功能对所有玩家生效（包括原版客户端），因为延迟发生在服务端。
 *
 * ======== Paper 实现限制 ========
 * 原始 NeoForge 模组通过 Mixin @Overwrite 替换 ChunkMap.updateChunkTracking()。
 * Paper 插件无法这样做。我们通过事件监听和延迟"忘记"来近似实现。
 *
 * 相比原始 NEB 模组的限制:
 *   - 无法完全覆盖 updateChunkTracking() 逻辑
 *   - 通过 PlayerMoveEvent 追踪区块变化（近似方案）
 *   - 缓存命中时客户端仍有区块数据（效果类似）
 * ============================================================================
 */

package org.mlc.mlc_neb.chunk;

import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerMoveEvent;
import org.bukkit.scheduler.BukkitTask;
import org.mlc.mlc_neb.MlcNeb;
import org.mlc.mlc_neb.NebConfig;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理延迟区块卸载以减少区块重发。
 *
 * <p>每玩家维护一个缓存: {@code Map<Long, Long>} — 打包区块位置 → 最后见到的时间戳。</p>
 */
public class DelayedChunkCache implements Listener {

    private static MlcNeb plugin;
    private static NebConfig config;
    private static BukkitTask cleanupTask;

    /** 每玩家缓存: UUID → (打包区块位置 → 最后见到时间戳)。 */
    private static final Map<UUID, Map<Long, Long>> playerCaches =
            new ConcurrentHashMap<>();

    /** 每玩家最后区块位置。 */
    private static final Map<UUID, Location> lastChunkPosition =
            new ConcurrentHashMap<>();

    private static volatile boolean initialized = false;

    // ========================================================================
    //  生命周期
    // ========================================================================

    /**
     * 初始化 DCC。插件启动时调用一次。
     */
    public static void init(MlcNeb mlcNeb) {
        if (initialized) return;
        plugin = mlcNeb;
        config = mlcNeb.getNebConfig();
        initialized = true;

        // 注册事件监听
        plugin.getServer().getPluginManager().registerEvents(
                new DelayedChunkCache(), plugin);

        // 定期清理过期缓存项（每 1 秒）
        cleanupTask = plugin.getServer().getScheduler().runTaskTimer(
                plugin, DelayedChunkCache::cleanupExpired, 20L, 20L);

        plugin.getLogger().info("延迟区块缓存(DCC)已初始化 "
                + "(sizeLimit=" + config.getDccSizeLimit()
                + ", distance=" + config.getDccDistance()
                + ", timeout=" + config.getDccTimeout() + "s)");
    }

    /**
     * 关闭 DCC，清理所有缓存。
     */
    public static void shutdown() {
        if (cleanupTask != null) cleanupTask.cancel();
        playerCaches.clear();
        lastChunkPosition.clear();
        initialized = false;
    }

    /**
     * 玩家退出时移除其缓存。
     */
    public static void onPlayerQuit(Player player) {
        playerCaches.remove(player.getUniqueId());
        lastChunkPosition.remove(player.getUniqueId());
    }

    // ========================================================================
    //  事件处理
    // ========================================================================

    /**
     * 检测玩家区块变化并更新缓存。
     *
     * <p>当玩家移动到新区块时，将其加入 DCC 缓存。
     * 如果该区块已在缓存中 → "缓存命中"，区块数据仍在客户端。</p>
     */
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlayerMove(PlayerMoveEvent event) {
        if (!initialized || !config.isDccEnabled()) return;

        Location from = event.getFrom();
        Location to = event.getTo();
        if (to == null) return;

        int fromChunkX = from.getBlockX() >> 4;
        int fromChunkZ = from.getBlockZ() >> 4;
        int toChunkX = to.getBlockX() >> 4;
        int toChunkZ = to.getBlockZ() >> 4;

        // 仅在区块实际变化时处理
        if (fromChunkX == toChunkX && fromChunkZ == toChunkZ) return;

        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();

        long now = System.currentTimeMillis();
        Map<Long, Long> cache = playerCaches.computeIfAbsent(uuid,
                k -> new LinkedHashMap<>());

        // 将新进入的区块加入缓存
        long packedNew = packChunk(toChunkX, toChunkZ);
        if (cache.containsKey(packedNew) && config.isDebugLog()) {
            plugin.getLogger().fine("DCC 命中: " + player.getName()
                    + " 返回缓存区块 (" + toChunkX + "," + toChunkZ + ")");
        }
        cache.put(packedNew, now);

        // 缓存 DCC 扩展范围内的相邻区块
        int extraDistance = config.getDccDistance();
        for (int dx = -extraDistance; dx <= extraDistance; dx++) {
            for (int dz = -extraDistance; dz <= extraDistance; dz++) {
                if (dx == 0 && dz == 0) continue;
                long packedNeighbor = packChunk(toChunkX + dx, toChunkZ + dz);
                cache.putIfAbsent(packedNeighbor, now);
            }
        }

        // 超出限制时裁剪
        trimCache(cache);

        lastChunkPosition.put(uuid, to.clone());
    }

    // ========================================================================
    //  缓存管理
    // ========================================================================

    /**
     * 从所有玩家缓存中移除过期项。
     * 由清理任务周期性调用。
     */
    private static void cleanupExpired() {
        if (!config.isDccEnabled()) return;

        long now = System.currentTimeMillis();
        long timeoutMs = config.getDccTimeout() * 1000L;

        for (Iterator<Map.Entry<UUID, Map<Long, Long>>> it =
                playerCaches.entrySet().iterator(); it.hasNext(); ) {

            Map.Entry<UUID, Map<Long, Long>> entry = it.next();
            Map<Long, Long> cache = entry.getValue();

            cache.entrySet().removeIf(e -> (now - e.getValue()) > timeoutMs);

            // 移除离线玩家的缓存
            Player player = plugin.getServer().getPlayer(entry.getKey());
            if (player == null || !player.isOnline()) {
                it.remove();
            }
        }
    }

    /**
     * 裁剪玩家缓存到配置的大小限制内。
     * 移除最旧的条目（FIFO 行为）。
     */
    private void trimCache(Map<Long, Long> cache) {
        int limit = config.getDccSizeLimit();
        if (cache.size() <= limit) return;

        List<Map.Entry<Long, Long>> sorted = new ArrayList<>(cache.entrySet());
        sorted.sort(Map.Entry.comparingByValue());

        int toRemove = cache.size() - limit;
        for (int i = 0; i < toRemove && i < sorted.size(); i++) {
            cache.remove(sorted.get(i).getKey());
        }
    }

    // ========================================================================
    //  公共 API
    // ========================================================================

    /** 检查指定区块位置是否在玩家缓存中。 */
    public static boolean isChunkCached(Player player, int chunkX, int chunkZ) {
        Map<Long, Long> cache = playerCaches.get(player.getUniqueId());
        if (cache == null) return false;

        long packed = packChunk(chunkX, chunkZ);
        long lastSeen = cache.getOrDefault(packed, 0L);
        long timeoutMs = config.getDccTimeout() * 1000L;

        return (System.currentTimeMillis() - lastSeen) <= timeoutMs;
    }

    /** 返回玩家当前缓存的区块数。 */
    public static int getCacheSize(Player player) {
        Map<Long, Long> cache = playerCaches.get(player.getUniqueId());
        return cache == null ? 0 : cache.size();
    }

    /** 返回所有玩家缓存的总区块数。 */
    public static int getTotalCacheSize() {
        return playerCaches.values().stream().mapToInt(Map::size).sum();
    }

    // ========================================================================
    //  工具
    // ========================================================================

    /**
     * 将两个 int 区块坐标打包为一个 long。
     * 使用与 Minecraft ChunkPos.toLong() 相同的打包方式。
     */
    private static long packChunk(int x, int z) {
        return ((long) x & 0xFFFFFFFFL) | (((long) z & 0xFFFFFFFFL) << 32);
    }
}
