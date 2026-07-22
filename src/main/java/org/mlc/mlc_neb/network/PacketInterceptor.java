/*
 * ============================================================================
 * mlc_neb — Netty 数据包拦截器
 * ============================================================================
 *
 * 这是 Paper API 和 Netty 网络层之间的关键桥梁。
 * 为了拦截所有 Minecraft 数据包（不仅是插件消息）、并拿到真实包类型，
 * 我们在玩家的 Netty 管道中注入自定义 handler，且注入点**位于 PacketEncoder 之前**。
 *
 * ======== 注入位置 ========
 *   ... → [neb_interceptor] → [packet_encoder] → [frame_prepender] → ...
 *
 * 此时 handler 的 write() 收到的 msg 仍是 NMS Packet<?> 对象（编码前的逻辑包），
 * 因此能通过反射拿到 packet.type().id() 的真实 "namespace:path" 标识，
 * 据此做黑名单过滤与缓冲。 这是与客户端 NotEnoughBandwidth 端 ParcelEncoder/
 * ConnectionMixin 同样的"编码前ивать"拦截点。
 *
 * 对于 NEB 启用玩家:
 *   - 黑名单/系统黑名单包（keep_alive, login, configuration_acknowledged, NEB 自身包…）
 *     → 立即放行（super.write），以保持时序正确。
 *   - BundlePacket → 拆包逐个按相同规则处理（保持顺序）。
 *   - 其余包 → 加入缓冲并取消继续向下传递（promise.setSuccess()，不下传）。
 *   - 每 20ms 刷新: 拼接+压缩 → 构造 NMS ClientboundCustomPayloadPacket(neb) 通过
 *     Connection.send 走正常 encoder 发出。
 *
 * 对于非 NEB 玩家:
 *   - 此拦截器根本不注入，所有包照常流通。
 *
 * ======== 全反射 NMS ========
 * 不引入 paperweight 开发包; Packet 对象编译期以 Object 承载，类型与成员
 * 通过 {@link NmsReflect} 反射访问。 见该类顶部说明。
 * ============================================================================
 */

package org.mlc.mlc_neb.network;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.bukkit.entity.Player;
import org.mlc.mlc_neb.MlcNeb;
import org.mlc.mlc_neb.aggregation.AggregationManager;
import org.mlc.mlc_neb.util.PlayerState;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 在玩家 Netty 管道中注入和管理 NEB 数据包拦截器（编码前）。
 */
public class PacketInterceptor extends ChannelOutboundHandlerAdapter {

    /** 拦截器在管道中注册的名字。 */
    public static final String HANDLER_NAME = "neb_interceptor";

    private static final Logger logger = Logger.getLogger("mlc_neb");

    /** 本拦截器所属的玩家状态。 */
    private final PlayerState state;

    /** 聚合管理器引用。 */
    private final AggregationManager aggregationManager;

    /** 插件引用。 */
    private final MlcNeb plugin;

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
     * 在主线程或握手回调中调用; channel 暂未就绪则延迟重试。
     */
    public static void inject(MlcNeb plugin, Player player, PlayerState state) {
        if (!NmsReflect.isAvailable()) {
            logger.warning("NMS 反射不可用，无法为 " + player.getName()
                    + " 注入 NEB 拦截器。该玩家将走原版路径。");
            return;
        }
        Channel channel = NmsReflect.getChannel(player);
        if (channel == null || !channel.isActive()) {
            // Channel 还没准备好 — 延迟重试
            plugin.getServer().getScheduler().runTaskLater(plugin,
                    () -> inject(plugin, player, state), 10L);
            return;
        }

        state.setChannel(channel);

        // 已注入则跳过
        if (channel.pipeline().get(HANDLER_NAME) != null) {
            return;
        }

        // 找到 PacketEncoder 在管道中的名字
        String encoderName = findEncoderName(channel.pipeline());
        if (encoderName == null) {
            logger.warning("在 " + player.getName()
                    + " 的管道中未找到 PacketEncoder，NEB 聚合不可用。");
            return;
        }

        try {
            // 在 encoder 之前添加拦截器（此时能拿到原始 Packet<?> 对象）
            channel.pipeline().addBefore(encoderName, HANDLER_NAME,
                    new PacketInterceptor(state));
            if (plugin.getNebConfig().isDebugLog()) {
                logger.info("NEB 拦截器已注入到 " + player.getName()
                        + " 的管道 (在 '" + encoderName + "' 之前)");
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "注入 NEB 拦截器到 " + player.getName() + " 失败", e);
        }
    }

    // ========================================================================
    //  Netty Handler: 出站（服务端 → 客户端，编码前）
    // ========================================================================

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {

        // 我们注入在 PacketEncoder 之前,所以这里的 msg 是编码前的 NMS Packet<?> 对象,
        // 而非编码后的 ByteBuf。 这正是本拦截器能拿到真实包类型的前提。
        // (原版 Author 在 encoder 之后注入只能看到裸字节,被迫把 typeId 写成 "unknown".)
        if (!NmsReflect.getPacketClass().isInstance(msg)) {
            // 非 Packet:可能是心跳握手的原始 ByteBuf 或其它控制帧,直接放行。
            super.write(ctx, msg, promise);
            return;
        }

        try {
            handlePacket(ctx, msg, promise);
        } catch (Throwable t) {
            // 关键不变量:任何异常都不得阻塞玩家的 Netty 管道。
            // NEB 是优化层,绝不能让玩家因本插件的 bug 而断线。
            // 因此出错时回退为放行原包,代价是该包不参与本批次聚合。
            logger.log(Level.WARNING, "NEB 拦截器处理包失败，放行原包", t);
            super.write(ctx, msg, promise);
        }
    }

    /**
     * 处理一个 NMS Packet: 黑名单/系统黑名单放行，Bundle 拆开，其余缓冲并取消下传。
     */
    private void handlePacket(ChannelHandlerContext ctx, Object packet, ChannelPromise promise)
            throws Throwable {

        // 通过反射 packet.type().id().toString() 拿 "namespace:path"。
        // 这是后续黑名单过滤与客户端解包时识别子包类型的唯一依据。
        String typeId = NmsReflect.getPacketTypeId(packet);

        // 拿不到类型 → 保守放行(不缓冲)。 宁可不优化也不能错放/错聚合。
        if (typeId == null) {
            super.write(ctx, packet, promise);
            return;
        }

        // BundlePacket: 一个 bundle 内含多个顺序紧密的子包(如多包同 tick 发送)。
        // 不能整个缓冲(bundle 内子包可能在 flush 时跨帧、破坏原子性),
        // 也不能直接放行(否则 bundle 外层若被缓冲,子包顺序与外层失落配)。
        // 正确做法:拆开逐个按相同规则处理。
        if (isBundlePacket(packet)) {
            forwardBundle(ctx, packet, promise);
            return;
        }

        // 系统黑名单(keep_alive/login/configuration_acknowledged/NEB 自身包等)
        // 与兼容黑名单(command_suggestions 等)→ 必须立即放行以保持时序。
        // 关键:命中黑名单时先 flush 当前已缓冲的包,再放行本包。
        // 否则黑名单包会跑到已缓冲包前面到达客户端,违反"聚合包到达顺序 == 原发送顺序"语义,
        // 客户端状态机会错乱(例如 login 包领先于之前的 level_chunk)。
        if (plugin.getNebConfig().shouldSkipAggregation(typeId)) {
            aggregationManager.flushPlayer(state);
            super.write(ctx, packet, promise);
            return;
        }

        // 普通可聚合包:存入缓冲,并以 setSuccess() 取消继续向下游(frame_prepender)传递。
        // 注意:promise.setSuccess() 表示"此次 write 视为成功",但 data 并未真正发出,
        // 它将在下次 flush 时作为聚合包的一部分发出。 此处的包对象不 release
        // (其生命周期由 Minecraft Connection 管理;我们不持有额外引用计数)。
        state.bufferPacket(packet);
        promise.setSuccess();
    }

    // ========================================================================
    //  BundlePacket 处理
    // ========================================================================

    private static boolean isBundlePacket(Object packet) {
        Class<?> c = packet.getClass();
        while (c != null) {
            if (c.getName().equals("net.minecraft.network.protocol.BundlePacket")) {
                return true;
            }
            c = c.getSuperclass();
        }
        return false;
    }

    /**
     * 将 BundlePacket 的子包逐一按相同规则处理。 外层 promise 在最后一个子包上兑现。
     */
    private void forwardBundle(ChannelHandlerContext ctx, Object bundle, ChannelPromise outer)
            throws Throwable {
        // 反射调用 BundlePacket.subPackets() -> Iterable<Packet<?>>
        Iterable<?> subPackets;
        try {
            java.lang.reflect.Method m = bundle.getClass().getMethod("subPackets");
            subPackets = (Iterable<?>) m.invoke(bundle);
        } catch (Exception e) {
            // 无法拆包 → 放行整 bundle
            super.write(ctx, bundle, outer);
            return;
        }

        boolean first = true;
        for (Object sub : subPackets) {
            if (first) {
                first = false;
            }
            // 用新 promise 处理每个子包，最后一个用 outer 兑现。
            ChannelPromise subPromise = (sub != null) ? ctx.newPromise() : outer;
            handlePacket(ctx, sub, subPromise);
        }
        // 若无子包，兑现 outer。
        if (first) {
            outer.setSuccess();
        }
    }

    // ========================================================================
    //  管道查找
    // ========================================================================

    /**
     * 在管道中查找 PacketEncoder handler 的名称。 通过类名搜索，因为 handler 名称可能变化。
     */
    private static String findEncoderName(io.netty.channel.ChannelPipeline pipeline) {
        for (var entry : pipeline) {
            String className = entry.getValue().getClass().getSimpleName();
            if (className.equals("PacketEncoder")) {
                return entry.getKey();
            }
        }
        // 候补: 模糊匹配与常见名
        for (var entry : pipeline) {
            String className = entry.getValue().getClass().getSimpleName();
            if (className.contains("PacketEncoder") || className.contains("Encoder")) {
                // 排除明显非 packet 的
                if (!className.contains("Frame") && !className.contains("Compression")) {
                    return entry.getKey();
                }
            }
        }
        if (pipeline.get("packet_encoder") != null) return "packet_encoder";
        if (pipeline.get("encoder") != null) return "encoder";
        return null;
    }
}