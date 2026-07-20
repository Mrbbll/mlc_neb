/*
 * ============================================================================
 * mlc_neb — /neb 命令
 * ============================================================================
 *
 * 服务端命令，用于查看 NEB 统计和插件管理。
 *
 * 子命令:
 *   /neb stats              — 显示全局带宽统计
 *   /neb stats <玩家>       — 显示指定玩家的统计
 *   /neb list               — 列出当前启用 NEB 的玩家
 *   /neb enable <玩家>      — 为指定玩家启用 NEB 优化（运行时）
 *   /neb disable <玩家>     — 禁用指定玩家的 NEB 优化（运行时）
 *   /neb reload             — 从 config.yml 热重载配置
 *   /neb version            — 显示插件版本
 *   /neb reset              — 重置所有统计计数器
 *
 * 权限节点:
 *   mlc_neb.command  — /neb 命令基本权限 (默认: op)
 *   mlc_neb.stats    — 查看统计 (默认: op)
 *   mlc_neb.reload   — 重载配置 (默认: op)
 *   mlc_neb.manage   — 启用/禁用玩家 NEB (默认: op)
 * ============================================================================
 */

package org.mlc.mlc_neb.command;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.command.*;
import org.bukkit.entity.Player;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mlc.mlc_neb.MlcNeb;
import org.mlc.mlc_neb.NebConfig;
import org.mlc.mlc_neb.chunk.DelayedChunkCache;
import org.mlc.mlc_neb.stats.StatsData;
import org.mlc.mlc_neb.stats.StatsManager;
import org.mlc.mlc_neb.util.PlayerState;

import java.util.*;
import java.util.stream.Collectors;

/**
 * /neb 命令的执行器和 Tab 补全器。
 */
public class NebCommand implements CommandExecutor, TabCompleter {

    private final MlcNeb plugin;
    private final NebConfig config;

    private static final String CMD_STATS = "stats";
    private static final String CMD_LIST = "list";
    private static final String CMD_ENABLE = "enable";
    private static final String CMD_DISABLE = "disable";
    private static final String CMD_RELOAD = "reload";
    private static final String CMD_VERSION = "version";
    private static final String CMD_RESET = "reset";

    public NebCommand(MlcNeb plugin) {
        this.plugin = plugin;
        this.config = plugin.getNebConfig();
    }

    // ========================================================================
    //  命令执行
    // ========================================================================

    @Override
    public boolean onCommand(@NotNull CommandSender sender,
                             @NotNull Command command,
                             @NotNull String label,
                             @NotNull String[] args) {

        if (args.length == 0) {
            return handleStats(sender, null);
        }

        switch (args[0].toLowerCase()) {
            case CMD_STATS -> {
                String target = args.length >= 2 ? args[1] : null;
                return handleStats(sender, target);
            }
            case CMD_LIST -> { return handleList(sender); }
            case CMD_ENABLE -> {
                if (args.length < 2) {
                    sender.sendMessage("§c用法: /neb enable <玩家名>");
                    return true;
                }
                return handleEnable(sender, args[1]);
            }
            case CMD_DISABLE -> {
                if (args.length < 2) {
                    sender.sendMessage("§c用法: /neb disable <玩家名>");
                    return true;
                }
                return handleDisable(sender, args[1]);
            }
            case CMD_RELOAD -> { return handleReload(sender); }
            case CMD_VERSION -> { return handleVersion(sender); }
            case CMD_RESET -> { return handleReset(sender); }
            default -> {
                sender.sendMessage("§c未知子命令: /neb " + args[0]);
                sender.sendMessage("§7可用: stats, list, enable, disable, reload, version, reset");
                return true;
            }
        }
    }

    // ========================================================================
    //  Tab 补全
    // ========================================================================

    @Override
    public @Nullable List<String> onTabComplete(@NotNull CommandSender sender,
                                                 @NotNull Command command,
                                                 @NotNull String alias,
                                                 @NotNull String[] args) {
        if (args.length == 1) {
            String partial = args[0].toLowerCase();
            return List.of(CMD_STATS, CMD_LIST, CMD_ENABLE, CMD_DISABLE,
                            CMD_RELOAD, CMD_VERSION, CMD_RESET)
                    .stream()
                    .filter(s -> s.startsWith(partial))
                    .collect(Collectors.toList());
        }

        if (args.length == 2) {
            String sub = args[0].toLowerCase();
            if (sub.equals(CMD_STATS) || sub.equals(CMD_ENABLE)
                    || sub.equals(CMD_DISABLE)) {
                String partial = args[1].toLowerCase();
                return Bukkit.getOnlinePlayers().stream()
                        .map(Player::getName)
                        .filter(n -> n.toLowerCase().startsWith(partial))
                        .collect(Collectors.toList());
            }
        }

        return List.of();
    }

    // ========================================================================
    //  子命令: stats
    // ========================================================================

    private boolean handleStats(CommandSender sender, @Nullable String targetName) {
        if (targetName != null) {
            Player target = Bukkit.getPlayer(targetName);
            if (target == null) {
                sender.sendMessage("§c玩家未找到: " + targetName);
                return true;
            }
            return showPlayerStats(sender, target);
        } else {
            return showGlobalStats(sender);
        }
    }

    private boolean showPlayerStats(CommandSender sender, Player target) {
        UUID uuid = target.getUniqueId();
        StatsData stats = StatsManager.getPlayerStats(uuid);
        PlayerState state = plugin.getPlayerState(target);

        sender.sendMessage("§6═════ NEB 统计: §e" + target.getName() + " §6═════");
        sender.sendMessage("§7NEB 状态: "
                + (state != null && state.isNebEnabled() ? "§a✓ 已启用" : "§c✗ 未启用"));
        sender.sendMessage("§7DCC 缓存区块: §f" + DelayedChunkCache.getCacheSize(target));

        if (stats != null && state != null) {
            sender.sendMessage("§b--- 实际传输（线字节）---");
            sender.sendMessage(String.format(
                    "  §7↓ 入站  §f%-10s  §7总计 §f%-12s    §7↑ 出站  §f%-10s  §7总计 §f%s",
                    formatSpeed((long) stats.inboundSpeedBaked.averageIn1s()),
                    formatSize(stats.inboundBytesBaked.get()),
                    formatSpeed((long) stats.outboundSpeedBaked.averageIn1s()),
                    formatSize(stats.outboundBytesBaked.get())
            ));

            sender.sendMessage("§b--- 原始负载（压缩前）---");
            sender.sendMessage(String.format(
                    "  §7↓ 入站  §f%-10s  §7总计 §f%-12s    §7↑ 出站  §f%-10s  §7总计 §f%s",
                    formatSpeed((long) stats.inboundSpeedRaw.averageIn1s()),
                    formatSize(stats.inboundBytesRaw.get()),
                    formatSpeed((long) stats.outboundSpeedRaw.averageIn1s()),
                    formatSize(stats.outboundBytesRaw.get())
            ));

            double ratio = stats.getOutboundRatio();
            double saved = 100.0 - ratio;
            sender.sendMessage(String.format(
                    "§a出站压缩率: §f%.1f%% §7(§a%.1f%% 带宽节省!§7)", ratio, saved));
        } else {
            sender.sendMessage("§7暂无统计数据。");
        }

        return true;
    }

    private boolean showGlobalStats(CommandSender sender) {
        int nebCount = 0;
        int totalOnline = 0;
        for (Player p : Bukkit.getOnlinePlayers()) {
            totalOnline++;
            PlayerState state = plugin.getPlayerState(p);
            if (state != null && state.isNebEnabled()) nebCount++;
        }

        sender.sendMessage("§6═════ NEB 全局统计 §6═════");
        sender.sendMessage("§7在线玩家: §f" + totalOnline + "  §7NEB 启用: §f" + nebCount);
        sender.sendMessage("§7DCC 总缓存区块: §f" + DelayedChunkCache.getTotalCacheSize());
        sender.sendMessage("§7聚合: §f"
                + (config.isAggregationEnabled() ? "§a启用" : "§c禁用")
                + "  §7刷新间隔: §f" + config.getFlushIntervalMs() + "ms"
                + "  §7压缩: §fZstd 级别 " + config.getCompressionLevel());

        sender.sendMessage("§b--- 实际传输（线字节）---");
        sender.sendMessage(String.format(
                "  §7↓ 入站  §f%-10s  §7总计 §f%-12s    §7↑ 出站  §f%-10s  §7总计 §f%s",
                formatSpeed((long) StatsManager.getGlobalInboundSpeedBaked()),
                formatSize(StatsManager.getGlobalInboundBaked()),
                formatSpeed((long) StatsManager.getGlobalOutboundSpeedBaked()),
                formatSize(StatsManager.getGlobalOutboundBaked())
        ));

        sender.sendMessage("§b--- 原始负载（压缩前）---");
        sender.sendMessage(String.format(
                "  §7↓ 入站  §f%-10s  §7总计 §f%-12s    §7↑ 出站  §f%-10s  §7总计 §f%s",
                formatSpeed((long) StatsManager.getGlobalInboundSpeedRaw()),
                formatSize(StatsManager.getGlobalInboundRaw()),
                formatSpeed((long) StatsManager.getGlobalOutboundSpeedRaw()),
                formatSize(StatsManager.getGlobalOutboundRaw())
        ));

        double ratio = StatsManager.getGlobalOutboundRatio();
        double saved = 100.0 - ratio;
        sender.sendMessage(String.format(
                "§a全局出站压缩率: §f%.1f%% §7(§a%.1f%% 带宽节省!§7)", ratio, saved));

        if (config.isDebugLog() && nebCount > 0) {
            sender.sendMessage("§7--- 每玩家缓冲状态 ---");
            for (Player p : Bukkit.getOnlinePlayers()) {
                PlayerState state = plugin.getPlayerState(p);
                if (state != null && state.isNebEnabled()) {
                    int bufSize = state.getPacketBuffer().size();
                    sender.sendMessage(String.format(
                            "  §a%s §7缓冲中: §f%d 包",
                            p.getName(), bufSize));
                }
            }
        }

        return true;
    }

    // ========================================================================
    //  子命令: list
    // ========================================================================

    private boolean handleList(CommandSender sender) {
        var enabledPlayers = config.getNebEnabledPlayers();

        sender.sendMessage("§6═════ NEB 启用玩家列表 §6═════");
        if (enabledPlayers.isEmpty()) {
            sender.sendMessage("§7无。使用 /neb enable <玩家> 或配置"
                    + " config.yml 中的 neb-enabled-players 添加。");
        } else {
            for (String uuidStr : enabledPlayers) {
                UUID uuid = UUID.fromString(uuidStr);
                Player p = Bukkit.getPlayer(uuid);
                String name = (p != null) ? p.getName() : "§7(离线)§f";
                PlayerState state = (p != null) ? plugin.getPlayerState(p) : null;
                String status = (state != null && state.isNebEnabled())
                        ? "§a[活跃]" : "§7[离线/未生效]";
                sender.sendMessage("  " + status + " §f" + name
                        + " §7(" + uuidStr + ")");
            }
        }
        return true;
    }

    // ========================================================================
    //  子命令: enable / disable
    // ========================================================================

    private boolean handleEnable(CommandSender sender, String targetName) {
        if (!sender.hasPermission("mlc_neb.manage")) {
            sender.sendMessage("§c你没有管理 NEB 玩家的权限。");
            return true;
        }

        Player target = Bukkit.getPlayer(targetName);
        if (target == null) {
            // 尝试按 UUID 添加
            try {
                UUID uuid = UUID.fromString(targetName);
                config.addNebPlayer(uuid.toString());
                sender.sendMessage("§a已添加 UUID " + uuid + " 到 NEB 启用列表。"
                        + " 玩家上线后将自动启用 NEB。");
                return true;
            } catch (IllegalArgumentException e) {
                sender.sendMessage("§c玩家未找到或 UUID 格式无效: " + targetName);
                return true;
            }
        }

        String uuid = target.getUniqueId().toString();
        config.addNebPlayer(uuid);

        PlayerState state = plugin.getPlayerState(target);
        if (state != null && !state.isNebEnabled()) {
            plugin.enableNebForPlayer(target, state);
            sender.sendMessage("§a已为 " + target.getName() + " 启用 NEB 优化。");
        } else {
            sender.sendMessage("§e" + target.getName() + " 已在 NEB 列表中。");
        }
        return true;
    }

    private boolean handleDisable(CommandSender sender, String targetName) {
        if (!sender.hasPermission("mlc_neb.manage")) {
            sender.sendMessage("§c你没有管理 NEB 玩家的权限。");
            return true;
        }

        Player target = Bukkit.getPlayer(targetName);
        if (target == null) {
            try {
                UUID uuid = UUID.fromString(targetName);
                config.removeNebPlayer(uuid.toString());
                sender.sendMessage("§a已从 NEB 启用列表移除 " + uuid + "。");
                return true;
            } catch (IllegalArgumentException e) {
                sender.sendMessage("§c玩家未找到或 UUID 格式无效: " + targetName);
                return true;
            }
        }

        config.removeNebPlayer(target.getUniqueId().toString());
        plugin.disableNebForPlayer(target);
        sender.sendMessage("§a已为 " + target.getName() + " 禁用 NEB 优化。");
        return true;
    }

    // ========================================================================
    //  子命令: reload
    // ========================================================================

    private boolean handleReload(CommandSender sender) {
        if (!sender.hasPermission("mlc_neb.reload")) {
            sender.sendMessage("§c你没有重载 NEB 配置的权限。");
            return true;
        }

        // 重新加载配置
        config.load();

        // 重新检查所有在线玩家
        for (Player p : Bukkit.getOnlinePlayers()) {
            PlayerState state = plugin.getPlayerState(p);
            if (state == null) continue;

            boolean shouldEnable = config.isPlayerNebEnabled(p.getUniqueId().toString());
            if (shouldEnable && !state.isNebEnabled()) {
                plugin.enableNebForPlayer(p, state);
            } else if (!shouldEnable && state.isNebEnabled()) {
                plugin.disableNebForPlayer(p);
            }
        }

        sender.sendMessage("§aNEB 配置已重载。");
        return true;
    }

    // ========================================================================
    //  子命令: version
    // ========================================================================

    private boolean handleVersion(CommandSender sender) {
        sender.sendMessage("§6mlc_neb §fv" + plugin.getPluginMeta().getVersion());
        sender.sendMessage("§7NEB (Not Enough Bandwidth) Paper 插件移植版");
        sender.sendMessage("§7原始 NeoForge 模组 by USS_Shenzhou");
        sender.sendMessage("§7协议版本: §f" + MlcNeb.PROTOCOL_VERSION);
        sender.sendMessage("§7Zstd: §f级别 " + config.getCompressionLevel()
                + ", 窗口 " + config.getContextLevel());
        sender.sendMessage("§7NEB 启用玩家数: §f" + config.getNebEnabledPlayers().size());
        return true;
    }

    // ========================================================================
    //  子命令: reset
    // ========================================================================

    private boolean handleReset(CommandSender sender) {
        if (!sender.hasPermission("mlc_neb.reload")) {
            sender.sendMessage("§c你没有重置 NEB 统计的权限。");
            return true;
        }

        StatsManager.resetAll();
        for (Player p : Bukkit.getOnlinePlayers()) {
            PlayerState state = plugin.getPlayerState(p);
            if (state != null) state.resetStats();
        }
        sender.sendMessage("§aNEB 统计已重置。");
        return true;
    }

    // ========================================================================
    //  格式化工具
    // ========================================================================

    /** 格式化为人类可读的速度（字节/秒）。 */
    private static String formatSpeed(long bytesPerSec) {
        if (bytesPerSec < 1000) {
            return String.format("%d §7B/S§r", bytesPerSec);
        } else if (bytesPerSec < 1000 * 1000) {
            return String.format("%.1f §7KiB/S§r", bytesPerSec / 1024.0);
        } else if (bytesPerSec < 1000 * 1000 * 1000) {
            return String.format("%.2f §7MiB/S§r",
                    bytesPerSec / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f §7GiB/S§r",
                    bytesPerSec / (1024.0 * 1024.0 * 1024.0));
        }
    }

    /** 格式化为人类可读的大小。 */
    private static String formatSize(long bytes) {
        if (bytes < 1000) {
            return bytes + " §7B§r";
        } else if (bytes < 1000 * 1000) {
            return String.format("%.1f §7KiB§r", bytes / 1024.0);
        } else if (bytes < 1000 * 1000 * 1000) {
            return String.format("%.2f §7MiB§r", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f §7GiB§r",
                    bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }
}
