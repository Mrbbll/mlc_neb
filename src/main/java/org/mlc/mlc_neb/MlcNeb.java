/*
 * ============================================================================
 * mlc_neb — Not Enough Bandwidth (NEB) — 插件主类
 * ============================================================================
 *
 * 原始 NeoForge 模组: "Not Enough Bandwidth" by USS_Shenzhou
 * 客户端模组路径: D:\desktop\plugins\mlc_neb\NotEnoughBandwidth
 *
 * Paper 插件移植: Mr_bl
 *
 * ======== 功能概述 ========
 *
 * 功能1 — 数据包聚合 + Zstd 压缩
 *   每 20ms 将 NEB 玩家的出站数据包缓冲合并为一个大数据包，
 *   经 Zstd 压缩后发送。客户端 NEB 模组自动解压→拆分→注入管道。
 *   原版玩家不受影响——数据包照常发送。
 *
 * 功能2 — 延迟区块缓存 (DCC)
 *   玩家移动时延迟"忘记区块"操作。纯服务端，对所有玩家生效。
 *
 * 功能3 — 流量统计
 *   /neb stats — 显示每玩家和全局带宽使用情况
 *   /neb enable/disable <玩家> — 管理 NEB 玩家
 *
 * ======== 握手检测 ========
 * 通过 Bukkit Plugin Message Channel "neb:handshake" 实现:
 *   服务端 → 客户端: "NEB|1.0|hello"
 *   客户端 → 服务端: "NEB|1.0|ack"
 *
 * 当前 NEB 模组未实现握手响应，超时后回退到配置文件判断。
 * 未来的 NEB 模组版本添加握手支持后，自动检测将无缝生效。
 * ============================================================================
 */

package org.mlc.mlc_neb;

import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.plugin.messaging.Messenger;
import org.bukkit.plugin.messaging.PluginMessageListener;
import org.bukkit.scheduler.BukkitTask;
import org.jetbrains.annotations.NotNull;
import org.mlc.mlc_neb.aggregation.AggregationManager;
import org.mlc.mlc_neb.chunk.DelayedChunkCache;
import org.mlc.mlc_neb.command.NebCommand;
import org.mlc.mlc_neb.handshake.HandshakeManager;
import org.mlc.mlc_neb.stats.StatsManager;
import org.mlc.mlc_neb.util.PlayerState;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 插件主入口。
 *
 * <p>生命周期:
 * <ol>
 *   <li>{@link #onEnable()} — 加载配置，注册插件消息通道，
 *       初始化各子系统，启动聚合刷新定时器。</li>
 *   <li>玩家加入 — 创建 PlayerState，发送握手 hello，等待 ack。
 *       收到 ack → 自动启用 NEB。超时 → 回退到配置文件判断。</li>
 *   <li>玩家退出 — 刷新缓冲，清理资源。</li>
 *   <li>{@link #onDisable()} — 刷新所有缓冲，关闭子系统。</li>
 * </ol>
 */
public final class MlcNeb extends JavaPlugin implements Listener, PluginMessageListener {

    // ---- 单例 ----
    private static MlcNeb instance;

    // ---- 协议版本 ----
    public static final String PROTOCOL_VERSION = "1.0";

    // ---- 插件消息通道标识 ----
    /** 握手检测通道。 */
    public static final String CHANNEL_HANDSHAKE = "neb:handshake";

    /** NEB 客户端模组注册的聚合包类型标识。
     *  必须与 NotEnoughBandwidth 中的 PacketAggregationPacket.TYPE 一致。 */
    public static final String NEB_PACKET_TYPE = "neb:packet_aggregation_packet";

    /** 握手超时（ticks），默认 60 ticks = 3 秒。 */
    public static final long HANDSHAKE_TIMEOUT_TICKS = 60L;

    // ---- 子系统 ----
    private NebConfig config;
    private AggregationManager aggregationManager;
    private HandshakeManager handshakeManager;
    private BukkitTask flushTask;

    /** 所有在线玩家的状态映射。 */
    private final Map<UUID, PlayerState> playerStates = new ConcurrentHashMap<>();

    // ========================================================================
    //  生命周期
    // ========================================================================

    @Override
    public void onEnable() {
        instance = this;

        // ---- 第1步: 加载配置 ----
        saveDefaultConfig();
        this.config = new NebConfig(this);
        getLogger().info("配置加载完成。");

        // ---- 第2步: 注册插件消息通道 ----
        Messenger messenger = getServer().getMessenger();
        messenger.registerOutgoingPluginChannel(this, CHANNEL_HANDSHAKE);
        messenger.registerIncomingPluginChannel(this, CHANNEL_HANDSHAKE, this);
        getLogger().info("插件消息通道已注册: " + CHANNEL_HANDSHAKE);

        // ---- 第3步: 初始化子系统 ----
        this.aggregationManager = new AggregationManager(this);
        this.handshakeManager = new HandshakeManager(this);

        // 延迟区块缓存 — 纯服务端，对所有玩家生效
        DelayedChunkCache.init(this);

        // 流量统计
        StatsManager.init();

        // ---- 第4步: 注册事件和命令 ----
        getServer().getPluginManager().registerEvents(this, this);

        NebCommand nebCommand = new NebCommand(this);
        var cmd = getCommand("neb");
        if (cmd != null) {
            cmd.setExecutor(nebCommand);
            cmd.setTabCompleter(nebCommand);
        }

        // ---- 第5步: 启动聚合刷新定时器 ----
        long interval = Math.max(1, Math.round(config.getFlushIntervalMs() / 50.0));
        flushTask = Bukkit.getScheduler().runTaskTimer(
                this, this::flushAllPlayers, interval, interval);
        getLogger().info("聚合刷新调度器已启动 (间隔: "
                + config.getFlushIntervalMs() + "ms / " + interval + " ticks)");

        // ---- 第6步: 对已在线玩家初始化 ----
        for (Player player : Bukkit.getOnlinePlayers()) {
            onPlayerJoinInternal(player);
        }

        getLogger().info("mlc_neb v" + getPluginMeta().getVersion() + " 已启用。");
        getLogger().info("握手检测: 自动 (超时" + HANDSHAKE_TIMEOUT_TICKS + "ticks)"
                + " | 强制全部启用: " + (config.isForceEnableAll() ? "是" : "否")
                + " | 手动列表: " + config.getNebEnabledPlayers().size() + " 名玩家");
        printEnabledPlayers();
    }

    @Override
    public void onDisable() {
        // 刷新所有待发数据包
        flushAllPlayers();

        if (flushTask != null) {
            flushTask.cancel();
        }

        if (aggregationManager != null) {
            aggregationManager.shutdown();
        }

        DelayedChunkCache.shutdown();

        // 取消所有等待中的握手
        for (UUID uuid : playerStates.keySet()) {
            handshakeManager.onPlayerQuit(uuid);
        }

        // 清理所有玩家状态
        for (PlayerState state : playerStates.values()) {
            state.cleanup();
        }
        playerStates.clear();

        // 注销通道
        Messenger messenger = getServer().getMessenger();
        messenger.unregisterOutgoingPluginChannel(this, CHANNEL_HANDSHAKE);
        messenger.unregisterIncomingPluginChannel(this, CHANNEL_HANDSHAKE);

        instance = null;
        getLogger().info("mlc_neb 已禁用。");
    }

    // ========================================================================
    //  事件处理
    // ========================================================================

    @EventHandler(priority = EventPriority.LOW)
    public void onPlayerJoin(PlayerJoinEvent event) {
        onPlayerJoinInternal(event.getPlayer());
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onPlayerQuit(PlayerQuitEvent event) {
        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();

        handshakeManager.onPlayerQuit(uuid);

        PlayerState state = playerStates.remove(uuid);
        if (state != null) {
            if (state.isNebEnabled()) {
                aggregationManager.flushPlayer(state);
            }
            state.cleanup();
        }

        DelayedChunkCache.onPlayerQuit(player);
        StatsManager.removePlayer(uuid);
    }

    /**
     * 内部加入处理，登录和重载场景共用。
     */
    private void onPlayerJoinInternal(Player player) {
        UUID uuid = player.getUniqueId();

        // 创建每玩家状态
        PlayerState state = new PlayerState(player);
        playerStates.put(uuid, state);

        // 初始化统计追踪
        StatsManager.registerPlayer(uuid);

        // 检查是否强制全部启用
        if (config.isForceEnableAll()) {
            getServer().getScheduler().runTaskLater(this,
                    () -> enableNebForPlayer(player, state), 10L);
            getLogger().info("强制全部启用模式: " + player.getName() + " 已启用 NEB。");
            return;
        }

        // 检查是否在配置的 NEB 玩家列表中
        if (config.isPlayerNebEnabled(uuid.toString())) {
            getServer().getScheduler().runTaskLater(this,
                    () -> enableNebForPlayer(player, state), 10L);
            getLogger().info("配置列表匹配: " + player.getName() + " 已启用 NEB。");
            return;
        }

        // 发送握手检测
        handshakeManager.sendHandshake(player);
    }

    // ========================================================================
    //  插件消息监听（客户端 → 服务端）
    // ========================================================================

    @Override
    public void onPluginMessageReceived(@NotNull String channel,
                                        @NotNull Player player,
                                        byte @NotNull [] message) {

        if (!channel.equals(CHANNEL_HANDSHAKE)) return;

        // 收到客户端握手响应
        handshakeManager.handleResponse(player, message);
    }

    // ========================================================================
    //  NEB 启用/禁用
    // ========================================================================

    /**
     * 为指定玩家启用 NEB 优化（注入 Netty 拦截器，初始化压缩上下文）。
     *
     * @param player 目标玩家
     * @param state  玩家状态对象
     */
    public void enableNebForPlayer(Player player, PlayerState state) {
        if (state.isNebEnabled()) return;

        state.setNebEnabled(true);
        aggregationManager.initCompressor(state);

        // 注入 Netty 管道拦截器
        org.mlc.mlc_neb.network.PacketInterceptor.inject(this, player, state);

        if (config.isDebugLog()) {
            getLogger().info("玩家 " + player.getName() + " 已启用 NEB 优化。");
        }
    }

    /**
     * 禁用指定玩家的 NEB 优化（刷新缓冲、移除拦截器）。
     */
    public void disableNebForPlayer(Player player) {
        PlayerState state = playerStates.get(player.getUniqueId());
        if (state == null || !state.isNebEnabled()) return;

        aggregationManager.flushPlayer(state);
        state.setNebEnabled(false);
        state.cleanup();

        if (config.isDebugLog()) {
            getLogger().info("玩家 " + player.getName() + " 已禁用 NEB 优化。");
        }
    }

    // ========================================================================
    //  定时刷新
    // ========================================================================

    /**
     * 遍历所有 NEB 启用玩家，刷新其数据包缓冲区。
     */
    private void flushAllPlayers() {
        for (PlayerState state : playerStates.values()) {
            if (state.isNebEnabled() && state.getPlayer().isOnline()) {
                aggregationManager.flushPlayer(state);
            }
        }
    }

    // ========================================================================
    //  工具方法
    // ========================================================================

    private void printEnabledPlayers() {
        var players = config.getNebEnabledPlayers();
        if (!players.isEmpty()) {
            getLogger().info("配置中 NEB 启用玩家 (" + players.size() + " 人):");
            for (String uuid : players) {
                Player p = Bukkit.getPlayer(UUID.fromString(uuid));
                String name = (p != null) ? p.getName() : "(离线)";
                getLogger().info("  - " + name + " (" + uuid + ")");
            }
        }
    }

    // ========================================================================
    //  访问器
    // ========================================================================

    public static MlcNeb getInstance() { return instance; }
    public NebConfig getNebConfig() { return config; }
    public AggregationManager getAggregationManager() { return aggregationManager; }
    public HandshakeManager getHandshakeManager() { return handshakeManager; }

    public PlayerState getPlayerState(UUID uuid) { return playerStates.get(uuid); }
    public PlayerState getPlayerState(Player player) {
        return playerStates.get(player.getUniqueId());
    }
    public Map<UUID, PlayerState> getPlayerStates() { return playerStates; }
}
