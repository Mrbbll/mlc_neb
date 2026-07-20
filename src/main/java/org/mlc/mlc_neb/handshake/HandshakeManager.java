/*
 * ============================================================================
 * mlc_neb — 握手管理器（客户端 NEB 模组检测）
 * ============================================================================
 *
 * 通过 Bukkit Plugin Message Channel 实现握手检测协议:
 *
 *   服务端 → 客户端:  "NEB|{version}|hello"   通道 "neb:handshake"
 *   客户端 → 服务端:  "NEB|{version}|ack"     通道 "neb:handshake"
 *
 * Bukkit 的 Plugin Message Channel 是 Minecraft 协议层的机制，
 * 所有客户端（原版/Forge/NeoForg/Fabric）都能接收。
 * 任何实现了握手响应的客户端模组都可以被自动检测。
 *
 * ======== 与现有 NEB 模组的兼容性 ========
 * 当前的 NotEnoughBandwidth 客户端模组（NeoForg）没有握手响应处理器。
 * 这意味着它不会回复 ack，服务端会在超时后将玩家标记为"未检测到 NEB"。
 *
 * 解决方案:
 *   1. 自动模式: 握手超时 → 回退到 config.yml 中的 neb-enabled-players 列表
 *   2. 手动模式: 使用 /neb enable <玩家> 命令
 *   3. 全启用模式: 配置 force-enable-all: true 对所有玩家启用 NEB
 *
 * 当 NEB 模组未来版本添加握手支持后，自动检测将无缝生效。
 * ============================================================================
 */

package org.mlc.mlc_neb.handshake;

import org.bukkit.entity.Player;
import org.bukkit.scheduler.BukkitRunnable;
import org.bukkit.scheduler.BukkitTask;
import org.mlc.mlc_neb.MlcNeb;
import org.mlc.mlc_neb.util.PlayerState;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * 管理 NEB 客户端模组的握手检测。
 *
 * <p>玩家加入时发送 hello 消息。若客户端回复 ack，
 * 则自动启用 NEB 优化。超时未回复则回退到配置文件列表判断。</p>
 */
public class HandshakeManager {

    private final MlcNeb plugin;
    private final Logger logger;

    /** 握手 hello 消息字节（缓存避免重复分配）。 */
    private final byte[] helloMessage;

    /** 握手 ACK 期望前缀。 */
    private static final String ACK_PREFIX = "NEB|";
    private static final String ACK_SUFFIX = "|ack";

    /** 正在等待握手的玩家及其超时任务。 */
    private final Map<UUID, BukkitTask> pendingHandshakes = new ConcurrentHashMap<>();

    public HandshakeManager(MlcNeb plugin) {
        this.plugin = plugin;
        this.logger = plugin.getLogger();
        this.helloMessage = ("NEB|" + MlcNeb.PROTOCOL_VERSION + "|hello")
                .getBytes(StandardCharsets.UTF_8);
    }

    // ========================================================================
    //  发送握手
    // ========================================================================

    /**
     * 向玩家发送 NEB 握手 hello 并安排超时任务。
     * 在玩家加入服务器时调用。
     *
     * <p>延迟 5 ticks 发送以确保客户端的插件通道注册完成。
     * 超时在 {@link MlcNeb#HANDSHAKE_TIMEOUT_TICKS} ticks 后触发。</p>
     *
     * @param player 刚加入的玩家
     */
    public void sendHandshake(Player player) {
        PlayerState state = plugin.getPlayerState(player);
        if (state == null) return;

        UUID uuid = player.getUniqueId();

        // 取消之前可能存在的握手任务
        cancelPending(uuid);

        // 延迟发送 hello（确保客户端通道已就绪）
        BukkitTask sendTask = new BukkitRunnable() {
            @Override
            public void run() {
                if (!player.isOnline()) {
                    cancelPending(uuid);
                    return;
                }

                try {
                    // 通过 Bukkit 插件消息通道发送握手
                    // 这是 Minecraft 协议层的标准机制，所有客户端都能接收
                    player.sendPluginMessage(plugin,
                            MlcNeb.CHANNEL_HANDSHAKE, helloMessage);

                    if (plugin.getNebConfig().isDebugLog()) {
                        logger.fine("已向 " + player.getName()
                                + " 发送 NEB 握手 hello");
                    }
                } catch (Exception e) {
                    logger.warning("向 " + player.getName()
                            + " 发送 NEB 握手失败: " + e.getMessage());
                    cancelPending(uuid);
                }
            }
        }.runTaskLater(plugin, 5L);

        // 安排超时任务
        BukkitTask timeoutTask = new BukkitRunnable() {
            @Override
            public void run() {
                pendingHandshakes.remove(uuid);

                if (!player.isOnline()) return;
                PlayerState s = plugin.getPlayerState(player);
                if (s == null) return;

                // 握手超时 — 客户端未回复
                // 回退到配置文件列表判断
                boolean configEnabled = plugin.getNebConfig()
                        .isPlayerNebEnabled(uuid.toString());
                boolean forceAll = plugin.getNebConfig().isForceEnableAll();

                if (configEnabled || forceAll) {
                    // 配置中已启用 → 自动开启 NEB
                    plugin.enableNebForPlayer(player, s);
                    if (plugin.getNebConfig().isDebugLog()) {
                        logger.fine(player.getName()
                                + " 握手超时，但配置中已启用 NEB，强制开启。");
                    }
                } else {
                    // 未检测到 NEB 客户端模组 → 使用原版路径（透明兼容）
                    if (plugin.getNebConfig().isDebugLog()) {
                        logger.fine(player.getName()
                                + " 握手超时，未检测到 NEB 客户端模组。"
                                + "使用原版数据包路径。");
                    }
                }
            }
        }.runTaskLater(plugin, MlcNeb.HANDSHAKE_TIMEOUT_TICKS);

        pendingHandshakes.put(uuid, timeoutTask);
    }

    // ========================================================================
    //  处理响应
    // ========================================================================

    /**
     * 处理客户端发来的握手响应。
     *
     * <p>如果响应包含 "NEB|{version}|ack" 且版本兼容，
     * 则为该玩家启用 NEB 优化:
     * <ol>
     *   <li>标记玩家为 NEB 已启用</li>
     *   <li>注入 Netty 数据包拦截器到连接管道</li>
     *   <li>初始化 Zstd 压缩上下文</li>
     * </ol></p>
     *
     * @param player  发送响应的玩家
     * @param message 握手通道上收到的原始字节
     */
    public void handleResponse(Player player, byte[] message) {
        UUID uuid = player.getUniqueId();

        // 取消超时任务
        cancelPending(uuid);

        PlayerState state = plugin.getPlayerState(player);
        if (state == null) return;

        String response = new String(message, StandardCharsets.UTF_8);

        // 验证响应格式: "NEB|{version}|ack"
        if (!response.startsWith(ACK_PREFIX) || !response.endsWith(ACK_SUFFIX)) {
            if (plugin.getNebConfig().isDebugLog()) {
                logger.fine(player.getName()
                        + " 发送了无效的 NEB 握手响应: " + response);
            }
            // 回退到配置文件判断
            fallbackToConfig(player, state);
            return;
        }

        // 提取并检查协议版本
        String[] parts = response.split("\\|");
        if (parts.length != 3) {
            logger.warning(player.getName()
                    + " 发送了格式错误的 NEB 握手: " + response);
            fallbackToConfig(player, state);
            return;
        }

        String clientVersion = parts[1];
        // 目前任何 1.x 版本都兼容
        if (!clientVersion.startsWith("1.")) {
            logger.warning(player.getName()
                    + " 的 NEB 协议版本不兼容: " + clientVersion);
            fallbackToConfig(player, state);
            return;
        }

        // ---- 握手成功 — 自动启用 NEB ----
        state.setHandshakeComplete(true);
        plugin.enableNebForPlayer(player, state);

        // 自动添加到启用列表（运行时）
        plugin.getNebConfig().addNebPlayer(uuid.toString());

        logger.info("✓ 检测到 " + player.getName()
                + " 安装了 NEB 客户端模组 (协议 v" + clientVersion
                + ")。聚合+压缩已自动启用。");
    }

    // ========================================================================
    //  回退逻辑
    // ========================================================================

    /**
     * 握手失败时回退到配置文件列表判断。
     */
    private void fallbackToConfig(Player player, PlayerState state) {
        UUID uuid = player.getUniqueId();
        boolean configEnabled = plugin.getNebConfig()
                .isPlayerNebEnabled(uuid.toString());
        boolean forceAll = plugin.getNebConfig().isForceEnableAll();

        if (configEnabled || forceAll) {
            plugin.enableNebForPlayer(player, state);
            if (plugin.getNebConfig().isDebugLog()) {
                logger.fine(player.getName()
                        + " 握手失败，但配置中已启用 NEB，强制开启。");
            }
        }
    }

    // ========================================================================
    //  工具
    // ========================================================================

    /** 取消等待中的握手超时任务。 */
    private void cancelPending(UUID uuid) {
        BukkitTask task = pendingHandshakes.remove(uuid);
        if (task != null && !task.isCancelled()) {
            task.cancel();
        }
    }

    /** 玩家退出时清理。 */
    public void onPlayerQuit(UUID uuid) {
        cancelPending(uuid);
    }
}
