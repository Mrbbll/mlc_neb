/*
 * ============================================================================
 * mlc_neb — Netty 数据包拦截器
 * ============================================================================
 *
 * 这是 Paper API 和 Netty 网络层之间的关键桥梁。
 * 为了拦截所有 Minecraft 数据包（不仅是插件消息），
 * 我们需要在玩家的 Netty 管道中注入自定义 handler。
 *
 * ======== 注入位置 ========
 * 在 PacketEncoder 之后、frame_prepender 之前添加 ChannelOutboundHandlerAdapter。
 * 这样我们拦截到的是已编码但未加长度前缀的 ByteBuf。
 *
 * 管道结构（简化）:
 *   ... → [packet_encoder] → [neb_interceptor] → [frame_prepender] → ...
 *
 * 对于 NEB 启用玩家:
 *   - Packet 经 encoder 编码为 ByteBuf → 到达 neb_interceptor
 *   - neb_interceptor 缓冲 ByteBuf（取消继续向下传递）
 *   - 每 20ms 刷新: 拼接+压缩 → 构造为 NEB 聚合负载 → 写入管道
 *
 * 对于非 NEB 玩家:
 *   - 此拦截器根本不会注入，所有包照常流通
 *
 * ======== NMS 反射 ========
 * Paper API 不直接暴露 Netty Channel。
 * 需要通过 CraftBukkit → NMS → Connection → Channel 链获取。
 * 这是许多 Paper 插件使用的标准技术。
 * ============================================================================
 */

package org.mlc.mlc_neb.network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.bukkit.entity.Player;
import org.mlc.mlc_neb.MlcNeb;
import org.mlc.mlc_neb.aggregation.AggregationManager;
import org.mlc.mlc_neb.util.PlayerState;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 在玩家 Netty 管道中注入和管理 NEB 数据包拦截器。
 *
 * <h4>工作原理</h4>
 * <p>每个 Minecraft 客户端连接有一个 Netty Channel 和一个 handler 管道。
 * 我们找到 PacketEncoder 在管道中的位置，把拦截器添加到它后面。
 * 当 Minecraft 发送数据包时，编码后的 ByteBuf 会先经过我们的 write() 方法。</p>
 */
public class PacketInterceptor extends ChannelOutboundHandlerAdapter {

    /** 拦截器在管道中注册的名字。 */
    public static final String HANDLER_NAME = "neb_interceptor";

    private static final Logger logger = MlcNeb.getInstance() != null
            ? MlcNeb.getInstance().getLogger()
            : Logger.getLogger("mlc_neb");

    /** 本拦截器所属的玩家状态。 */
    private final PlayerState state;

    /** 聚合管理器引用。 */
    private final AggregationManager aggregationManager;

    /** 插件引用。 */
    private final MlcNeb plugin;

    // ---- NMS 反射缓存 ----
    private static Class<?> craftPlayerClass;
    private static Method getHandleMethod;
    private static Field connectionField;

    static {
        try {
            initReflection();
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    "NMS 反射初始化失败，数据包拦截将不可用。", e);
        }
    }

    // ========================================================================
    //  构造
    // ========================================================================

    public PacketInterceptor(PlayerState state) {
        this.state = state;
        this.plugin = MlcNeb.getInstance();
        this.aggregationManager = plugin.getAggregationManager();
    }

    // ========================================================================
    //  注入
    // ========================================================================

    /**
     * 将拦截器注入指定玩家的 Netty Channel 管道。
     *
     * @param plugin 主插件实例
     * @param player Bukkit 玩家
     * @param state  玩家状态对象
     */
    public static void inject(MlcNeb plugin, Player player, PlayerState state) {
        try {
            Channel channel = getChannel(player);
            if (channel == null) {
                // Channel 还没准备好 — 延迟重试
                plugin.getServer().getScheduler().runTaskLater(plugin,
                        () -> inject(plugin, player, state), 10L);
                return;
            }

            state.setChannel(channel);

            // 检查是否已注入
            if (channel.pipeline().get(HANDLER_NAME) != null) {
                return;
            }

            // 找到 PacketEncoder 在管道中的名字
            String encoderName = findEncoderName(channel.pipeline());

            if (encoderName != null) {
                // 在 encoder 之后添加拦截器（拦截已编码的 ByteBuf）
                channel.pipeline().addAfter(encoderName, HANDLER_NAME,
                        new PacketInterceptor(state));
                if (plugin.getNebConfig().isDebugLog()) {
                    logger.fine("NEB 拦截器已注入到 " + player.getName()
                            + " 的管道 (在 '" + encoderName + "' 之后)");
                }
            } else {
                logger.warning("在 " + player.getName()
                        + " 的管道中未找到 PacketEncoder，NEB 聚合不可用。");
            }

        } catch (Exception e) {
            logger.log(Level.WARNING, "为 " + player.getName()
                    + " 注入 NEB 拦截器失败", e);
        }
    }

    // ========================================================================
    //  Netty Handler: 出站（服务端 → 客户端）
    // ========================================================================

    @Override
    public void write(ChannelHandlerContext ctx, Object msg,
                      ChannelPromise promise) throws Exception {

        // 拦截器在 PacketEncoder 之后，msg 是编码后的 ByteBuf
        if (!(msg instanceof ByteBuf buf)) {
            // 非 ByteBuf（不太可能但做防御性处理）
            super.write(ctx, msg, promise);
            return;
        }

        // 提取字节
        byte[] data = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), data);

        // 获取包类型（需要从 pipeline 上下文中推断）
        // 因为我们在 encoder 之后，只能看到已编码的字节。
        // 简化处理: 使用通用类型标识，所有非黑名单包都缓冲。
        // 完整的类型识别需要在 encoder 之前拦截 Packet 对象。

        // 将已编码的包数据缓冲起来
        aggregationManager.bufferPacket(state, "unknown", data);

        // 取消原写入（不再向下传递到 frame_prepender）
        promise.setSuccess();

        // 释放原始 buf（数据已被保存到缓冲）
        // buf 会被 Netty 自动释放，这里不需要手动 release
    }

    // ========================================================================
    //  NMS 反射
    // ========================================================================

    /**
     * 初始化获取玩家 Netty Channel 所需的反射链。
     *
     * <p>链: {@code CraftPlayer → ServerPlayer → ServerGamePacketListenerImpl → Connection → Channel}</p>
     */
    private static void initReflection() throws ClassNotFoundException,
            NoSuchMethodException, NoSuchFieldException {

        // Paper 1.21 映射
        craftPlayerClass = Class.forName(
                "org.bukkit.craftbukkit.v1_21_R3.entity.CraftPlayer");
        getHandleMethod = craftPlayerClass.getMethod("getHandle");

        // ServerPlayer.connection → ServerGamePacketListenerImpl
        Class<?> serverPlayerClass = getHandleMethod.getReturnType();
        connectionField = findField(serverPlayerClass, "connection", "c");
        if (connectionField != null) {
            connectionField.setAccessible(true);
        }

        logger.info("NMS 反射初始化成功。");
    }

    /**
     * 在类层次中按名称查找字段。
     */
    private static Field findField(Class<?> clazz, String... names) {
        for (String name : names) {
            try {
                Field f = clazz.getDeclaredField(name);
                f.setAccessible(true);
                return f;
            } catch (NoSuchFieldException ignored) {
            }
        }
        Class<?> superClass = clazz.getSuperclass();
        if (superClass != null) {
            return findField(superClass, names);
        }
        return null;
    }

    /**
     * 通过 NMS 反射获取 Bukkit Player 的 Netty Channel。
     *
     * @param player Bukkit 玩家
     * @return Netty Channel，获取失败返回 null
     */
    public static Channel getChannel(Player player) {
        try {
            Object craftPlayer = craftPlayerClass.cast(player);
            Object serverPlayer = getHandleMethod.invoke(craftPlayer);
            Object listener = connectionField.get(serverPlayer);

            // 从 listener → Connection → Channel
            // ServerGamePacketListenerImpl 中的 connection 字段
            Field connInListener = findField(listener.getClass(),
                    "connection", "e", "c");

            Object connection;
            if (connInListener != null) {
                connection = connInListener.get(listener);
            } else {
                // 尝试 getConnection() 方法
                Method getConnection = listener.getClass()
                        .getMethod("getConnection");
                connection = getConnection.invoke(listener);
            }

            // Connection.channel → Channel
            Field channelField = findField(connection.getClass(),
                    "channel", "m");
            if (channelField != null) {
                return (Channel) channelField.get(connection);
            }

            return null;

        } catch (Exception e) {
            logger.log(Level.FINE, "获取玩家 " + player.getName()
                    + " 的 Netty Channel 失败", e);
            return null;
        }
    }

    /**
     * 在管道中查找 PacketEncoder handler 的名称。
     * 通过类名搜索，因为 handler 名称可能变化。
     */
    private static String findEncoderName(ChannelPipeline pipeline) {
        for (var entry : pipeline) {
            String className = entry.getValue().getClass().getSimpleName();
            if (className.equals("PacketEncoder")
                    || className.contains("PacketEncoder")) {
                return entry.getKey();
            }
        }
        // 候补: 尝试标准名称
        if (pipeline.get("encoder") != null) return "encoder";
        if (pipeline.get("packet_encoder") != null) return "packet_encoder";

        return null;
    }
}
