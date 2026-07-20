/*
 * ============================================================================
 * mlc_neb — 插件配置管理
 * ============================================================================
 *
 * 配置值存储在 plugins/mlc_neb/config.yml 中。
 * 通过 /neb reload 或服务器重启载入。
 *
 * 配置结构对应原始 NeoForge 模组的 NotEnoughBandwidthConfig，
 * 适配 Paper 的 YAML 配置格式。
 * ============================================================================
 */

package org.mlc.mlc_neb;

import org.bukkit.configuration.file.FileConfiguration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * 持有 NEB 插件的所有可配置参数。
 *
 * <p>配置从 {@code config.yml} 读取，可通过 {@code /neb reload} 运行时重载。</p>
 *
 * <p>注意: NEB 玩家列表变更后，已在线玩家的状态切换需额外处理。</p>
 */
public class NebConfig {

    private final MlcNeb plugin;
    private final Logger logger;

    // ========================================================================
    //  NEB 启用玩家设置
    // ========================================================================

    /**
     * 强制为所有玩家启用 NEB（跳过握手检测）。
     * 适用于服务器上所有玩家都已安装 NEB 模组的场景。
     */
    private boolean forceEnableAll = false;

    /**
     * UUID 字符串集合，这些玩家将启用 NEB 聚合压缩。
     * 握手检测失败的玩家会回退到该列表判断。
     */
    private final Set<String> nebEnabledPlayers = new HashSet<>();

    // ========================================================================
    //  聚合 / 压缩设置
    // ========================================================================

    private boolean aggregationEnabled = true;
    private int flushIntervalMs = 20;
    private int compressionThreshold = 32;
    private int compressionLevel = 3;
    private int contextLevel = 23;
    private int maxPacketSize = 4 * 1024 * 1024;

    // ========================================================================
    //  黑名单 / 兼容性
    // ========================================================================

    private boolean compatibleMode = false;
    private final Set<String> blacklist = new HashSet<>();

    /**
     * 系统级黑名单 — 始终生效，不受 compatibleMode 影响。
     * 这些数据包永远不应被聚合（如 NEB 自己的包、登录/配置包、心跳包）。
     */
    public static final Set<String> SYSTEM_BLACKLIST = Set.of(
            // ---- 避免无限递归 ----
            "neb:packet_aggregation_packet",
            "neb:handshake",

            // ---- 登录/配置（必须有序到达）----
            "minecraft:login",
            "minecraft:finish_configuration",
            "minecraft:configuration_acknowledged",

            // ---- 协议级别 ----
            "minecraft:keep_alive"
    );

    // ========================================================================
    //  延迟区块缓存 (DCC) 设置
    // ========================================================================

    private boolean dccEnabled = true;
    private int dccSizeLimit = 60;
    private int dccDistance = 5;
    private int dccTimeout = 60;

    // ========================================================================
    //  调试
    // ========================================================================

    private boolean debugLog = false;

    /** 不使用 Zstd 上下文复用的玩家 UUID 集合。 */
    private final Set<String> playersDoNotUseContext = new HashSet<>();

    // ========================================================================
    //  构造与加载
    // ========================================================================

    public NebConfig(MlcNeb plugin) {
        this.plugin = plugin;
        this.logger = plugin.getLogger();
        plugin.saveDefaultConfig();
        load();
    }

    /**
     * (重新)加载所有配置值。
     * 在插件启用和 /neb reload 时调用。
     */
    public void load() {
        plugin.reloadConfig();
        FileConfiguration cfg = plugin.getConfig();

        // 强制全部启用
        forceEnableAll = cfg.getBoolean("force-enable-all", false);

        // NEB 启用玩家列表
        nebEnabledPlayers.clear();
        for (String uuid : cfg.getStringList("neb-enabled-players")) {
            try {
                // 验证 UUID 格式
                UUID.fromString(uuid.trim());
                nebEnabledPlayers.add(uuid.trim());
            } catch (IllegalArgumentException e) {
                logger.warning("无效的 UUID 格式，已跳过: " + uuid);
            }
        }

        // 聚合设置
        aggregationEnabled = cfg.getBoolean("aggregation.enabled", true);
        flushIntervalMs = clamp(cfg.getInt("aggregation.flush-interval-ms", 20), 10, 100);
        compressionThreshold = clamp(cfg.getInt("aggregation.compression-threshold", 32), 0, 1024);
        compressionLevel = clamp(cfg.getInt("aggregation.compression-level", 3), 1, 22);
        contextLevel = clamp(cfg.getInt("aggregation.context-level", 23), 21, 25);
        maxPacketSize = parseByteSize(cfg.getString("aggregation.max-packet-size", "4MB"));

        // 兼容性
        compatibleMode = cfg.getBoolean("compatibility.enabled", false);
        blacklist.clear();
        for (String entry : cfg.getStringList("compatibility.blacklist")) {
            blacklist.add(entry);
        }
        if (blacklist.isEmpty()) {
            applyDefaultBlacklist();
        }

        // DCC
        dccEnabled = cfg.getBoolean("chunk-cache.enabled", true);
        dccSizeLimit = clamp(cfg.getInt("chunk-cache.size-limit", 60), 0, 500);
        dccDistance = clamp(cfg.getInt("chunk-cache.distance", 5), 0, 16);
        dccTimeout = clamp(cfg.getInt("chunk-cache.timeout-seconds", 60), 0, 600);

        // 调试
        debugLog = cfg.getBoolean("debug.log", false);

        // 上下文排除
        playersDoNotUseContext.clear();
        for (String uuid : cfg.getStringList("context-exclusion.player-uuids")) {
            try {
                UUID.fromString(uuid.trim());
                playersDoNotUseContext.add(uuid.trim());
            } catch (IllegalArgumentException ignored) {
                // 占位符 UUID 可以忽略
            }
        }

        logger.info("配置已重载。强制全部启用: " + forceEnableAll
                + " | 手动列表: " + nebEnabledPlayers.size() + " 名玩家");
    }

    /** 填充默认黑名单。 */
    private void applyDefaultBlacklist() {
        blacklist.add("minecraft:command_suggestion");
        blacklist.add("minecraft:command_suggestions");
        blacklist.add("minecraft:commands");
        blacklist.add("minecraft:chat_command");
        blacklist.add("minecraft:chat_command_signed");
        blacklist.add("minecraft:player_info_update");
        blacklist.add("minecraft:player_info_remove");
    }

    // ========================================================================
    //  玩家 NEB 状态管理
    // ========================================================================

    /**
     * 检查指定 UUID 的玩家是否应启用 NEB。
     */
    public boolean isPlayerNebEnabled(String uuid) {
        return nebEnabledPlayers.contains(uuid);
    }

    /**
     * 添加玩家到 NEB 启用列表（运行时，不持久化到配置文件）。
     */
    public void addNebPlayer(String uuid) {
        nebEnabledPlayers.add(uuid);
    }

    /**
     * 从 NEB 启用列表移除玩家。
     */
    public void removeNebPlayer(String uuid) {
        nebEnabledPlayers.remove(uuid);
    }

    /** 获取 NEB 启用玩家 UUID 列表（只读）。 */
    public Set<String> getNebEnabledPlayers() {
        return Collections.unmodifiableSet(nebEnabledPlayers);
    }

    /** 是否强制为所有玩家启用 NEB（跳过握手检测）。 */
    public boolean isForceEnableAll() { return forceEnableAll; }

    // ========================================================================
    //  黑名单检查
    // ========================================================================

    /**
     * 检查给定数据包类型是否应跳过聚合（立即发送）。
     *
     * @param packetType 包类型标识，如 "minecraft:level_chunk_with_light"
     * @return true = 绕过聚合直发
     */
    public boolean shouldSkipAggregation(String packetType) {
        if (SYSTEM_BLACKLIST.contains(packetType)) {
            return true;
        }
        if (compatibleMode && blacklist.contains(packetType)) {
            return true;
        }
        return false;
    }

    // ========================================================================
    //  工具方法
    // ========================================================================

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    /**
     * 解析人类可读的字节大小字符串，如 "4MB", "512KB", "1024"。
     */
    private static int parseByteSize(String s) {
        s = s.trim().toUpperCase();
        try {
            if (s.endsWith("MB")) {
                return (int) (Double.parseDouble(s.replace("MB", "").trim()) * 1024 * 1024);
            } else if (s.endsWith("KB")) {
                return (int) (Double.parseDouble(s.replace("KB", "").trim()) * 1024);
            } else if (s.endsWith("B")) {
                return (int) Double.parseDouble(s.replace("B", "").trim());
            } else {
                return Integer.parseInt(s);
            }
        } catch (NumberFormatException e) {
            return 4 * 1024 * 1024; // 默认 4MB
        }
    }

    // ========================================================================
    //  Getter
    // ========================================================================

    public boolean isAggregationEnabled() { return aggregationEnabled; }
    public int getFlushIntervalMs() { return flushIntervalMs; }
    public int getCompressionThreshold() { return compressionThreshold; }
    public int getCompressionLevel() { return compressionLevel; }
    public int getContextLevel() { return contextLevel; }
    public int getMaxPacketSize() { return maxPacketSize; }
    public boolean isCompatibleMode() { return compatibleMode; }
    public Set<String> getBlacklist() { return blacklist; }
    public boolean isDccEnabled() { return dccEnabled; }
    public int getDccSizeLimit() { return dccSizeLimit; }
    public int getDccDistance() { return dccDistance; }
    public int getDccTimeout() { return dccTimeout; }
    public boolean isDebugLog() { return debugLog; }
    public MlcNeb getPlugin() { return plugin; }

    /**
     * 检查指定 UUID 的玩家是否应使用 Zstd 上下文复用。
     */
    public boolean shouldUseContext(String playerUuid) {
        return !playersDoNotUseContext.contains(playerUuid);
    }
}
