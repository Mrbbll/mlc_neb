/*
 * ============================================================================
 * mlc_neb — NEB 聚合包 Payload 句柄（全反射）
 * ============================================================================
 *
 * 客户端 NotEnoughBandwidth 模组在 NeoForge 注册了一个实现
 * net.minecraft.network.protocol.common.custom.CustomPacketPayload 的
 * PacketAggregationPacket， 其 type().id() == "neb:packet_aggregation_packet"，
 * 编码时直接把外层帧字节（[bool compressed][varint rawSize][zstd bytes]）写入
 * RegistryFriendlyByteBuf。
 *
 * 服务端要发出兼容客户端的聚合包，需构造一个 NMS
 * ClientboundCustomPayloadPacket，其内部 payload:
 *   - 实现 CustomPacketPayload 接口
 *   - type() 返回 Type<>(ResourceLocation("neb","packet_aggregation_packet"))
 *   - 提供 StreamCodec 用于编码（Paper 1.21 通过 codec() 方法取得）
 *
 * 这些类型在 paper-api(compileOnly) 中不可见，本类用反射动态生成一个
 * 实现 CustomPacketPayload 接口的代理类（Proxy.newProxyInstance 或字节码生成都可）。
 * 这里用 java.lang.reflect.Proxy 实现 SimplePayload。
 *
 * 编码策略: Paper 1.21 发送 CustomPayloadPacket 的路径最终会调用
 * payload 自身或其注册的 codec 把 payload 写进 FriendlyByteBuf。 由于 neb 类型未在
 * 服务端 NetworkRegistry 注册，标准发送会抛"unknown payload"。
 * 因此本类同时提供一个“裸写”发送通道 fallback {@link NebPayloadFrame} 直接在
 * Connection 的 channel 上将已格式化的外层帧作为原始自定义 payload 字节写线，
 * 复现客户端 encode 的字节布局，使客户端 NEB 模组按相同布局解码。
 * ============================================================================
 */

package org.mlc.mlc_neb.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 构造服务端侧的 NEB 聚合 payload 并包进 ClientboundCustomPayloadPacket（或裸字节回退）。
 *
 * <p>所有 NMS 访问通过 {@link NmsReflect}。 调用前需 {@link NmsReflect#init()} 成功。</p>
 */
public final class NebPayloadFactory {

    private static final Logger logger = Logger.getLogger("mlc_neb");

    /** NEB 聚合包的 Identifier: "neb:packet_aggregation_packet" */
    public static final String NEB_NAMESPACE = "neb";
    public static final String NEB_PATH = "packet_aggregation_packet";

    private static volatile boolean initialized = false;
    private static volatile boolean available = false;

    // 缓存的 Type<PacketAggregationPacket> 实例
    private static Object nebTypeInstance;

    // ClientboundCustomPayloadPacket(Class<?>, CustomPacketPayload) 构造器
    // 1.21 起 ClientboundCustomPayloadPacket 可能仅持 payload; 反射查找构造器。
    private static Constructor<?> cppPacketCtor;

    // ResourceLocation(实例) for neb type
    private static Object nebResourceLocation;

    private NebPayloadFactory() {}

    public static synchronized void init() {
        if (initialized) return;
        initialized = true;
        if (!NmsReflect.isAvailable()) {
            available = false;
            return;
        }
        try {
            Class<?> rlClass = NmsReflect.getResourceLocationClass();
            nebResourceLocation = NmsReflect.newResourceLocation(NEB_NAMESPACE, NEB_PATH);

            Class<?> cppTypeClass = NmsReflect.getCustomPayloadTypeClass();
            // Type 唯一构造器: Type(ResourceLocation)
            Constructor<?> typeCtor = null;
            for (Constructor<?> c : cppTypeClass.getDeclaredConstructors()) {
                if (c.getParameterCount() == 1
                        && rlClass.isAssignableFrom(c.getParameterTypes()[0])) {
                    typeCtor = c;
                    break;
                }
            }
            if (typeCtor == null) {
                // 兼容 Type(Type<?>...) 或其它形式: 取第一个参数为 ResourceLocation 的构造器
                for (Constructor<?> c : cppTypeClass.getDeclaredConstructors()) {
                    if (c.getParameterCount() >= 1
                            && rlClass.isAssignableFrom(c.getParameterTypes()[0])) {
                        typeCtor = c;
                        break;
                    }
                }
            }
            if (typeCtor == null) {
                throw new IllegalStateException(
                        "CustomPacketPayload.Type 构造器未找到");
            }
            typeCtor.setAccessible(true);
            nebTypeInstance = typeCtor.newInstance(nebResourceLocation);

            // ClientboundCustomPayloadPacket 构造器: 形参为 CustomPacketPayload（1.21 起多为此形）。
            Class<?> cppPacketClass = NmsReflect.getCustomPayloadPacketClass();
            Class<?> cppInterface = NmsReflect.getCustomPayloadInterface();
            for (Constructor<?> c : cppPacketClass.getDeclaredConstructors()) {
                if (c.getParameterCount() == 1
                        && cppInterface.isAssignableFrom(c.getParameterTypes()[0])) {
                    cppPacketCtor = c;
                    cppPacketCtor.setAccessible(true);
                    break;
                }
            }
            // 若未找到单参，则保存全供 fallback 取用。
            available = true;
            logger.info("NebPayloadFactory 初始化成功。");
        } catch (Throwable t) {
            available = false;
            logger.log(Level.SEVERE, "NebPayloadFactory 初始化失败 — 聚合包发送将走裸字节回退路径", t);
        }
    }

    public static boolean isAvailable() { return available; }

    /**
     * 构造一个 NMS ClientboundCustomPayloadPacket，其 payload 的外层帧字节为 frameBytes。
     * 若标准构造路径不可用，返回 null（调用方应使用 {@link #writeRawFrame} 回退）。
     *
     * @param frameBytes 已格式化的外层帧字节 ([bool compressed][varint rawSize(if comp)][data])
     */
    public static Object createAggregationPacket(byte[] frameBytes) {
        if (!available) return null;
        try {
            Object payloadProxy = makePayloadProxy(frameBytes);
            if (cppPacketCtor != null) {
                return cppPacketCtor.newInstance(payloadProxy);
            }
            return null;
        } catch (Throwable t) {
            logger.log(Level.FINE, "构造 ClientboundCustomPayloadPacket 失败，将使用回退", t);
            return null;
        }
    }

    /**
     * 生成实现 CustomPacketPayload 的代理实例。
     *
     * <p>代理如下方法:
     * <ul>
     *   <li>{@code type()} → 返回缓存 nebTypeInstance</li>
     *   <li>{@code codec()} / {@code streamCodec()} → 若被调用，返回 null（让发送路径不走标准 codec，
     *       而是由本框架通过其它途径保证字节写入）</li>
     * </ul>
     * 注意：Paper 1.21 的发送路径新版可能不再直接调用 payload 的 codec（改由
     * NetworkRegistry 查 codec），未注册的 neb 类型会失败。 因此本代理主要供
     * fallback 字节路径用作类型身份载体。</p>
     */
    private static Object makePayloadProxy(byte[] frameBytes) {
        Class<?> cppInterface = NmsReflect.getCustomPayloadInterface();
        InvocationHandler handler = (proxy, method, args) -> {
            String name = method.getName();
            if (name.equals("type")) {
                return nebTypeInstance;
            }
            if (name.equals("hashCode")) {
                return System.identityHashCode(proxy);
            }
            if (name.equals("equals")) {
                return proxy == args[0];
            }
            if (name.equals("toString")) {
                return "NebAggregationPayload{size=" + frameBytes.length + "}";
            }
            // 其余方法（codec 等）默认返回 null / 默认值
            return defaultValue(method.getReturnType());
        };
        return Proxy.newProxyInstance(
                cppInterface.getClassLoader(),
                new Class<?>[]{cppInterface},
                handler);
    }

    // ========================================================================
    //  回退: 直接把外层帧作为"自定义 payload 字节"写进 FriendlyByteBuf，
    //  并自动加上 PacketID(ClientboundCustomPayloadPacket) 前缀。
    //  采用对 channel 在 encoder 之前直接写一个已含 PacketID 的 ByteBuf，
    //  让下游 packet_encoder/frame_prepender 处理长度前缀。
    //  此路径在主功能发送前由 AggregationManager 决策采用。
    // ========================================================================

    /**
     * 构造一个可直接交给 player.channel().writeAndFlush 的 ByteBuf:
     * 内含 PacketID varint（ClientboundCustomPayloadPacket 的 ID） + payload。
     *
     * <p>由于无法从注册表稳定反查 neb 类型的 packet id，
     * 此回退方法仅做最稳妥的占位; 实际发送以 {@link #createAggregationPacket}
     * + Connection.send 为主路径。</p>
     */
    public static ByteBuf buildRawFrameBuf(byte[] frameBytes) {
        // 占位: 仅含 payload 字节, 无 PacketID 前缀。
        // 真正落地使用 createAggregationPacket 路径; 此处保留以备调试。
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(frameBytes.length);
        buf.writeBytes(frameBytes);
        return buf;
    }

    private static Object defaultValue(Class<?> rt) {
        if (rt == boolean.class) return false;
        if (rt == byte.class) return (byte) 0;
        if (rt == char.class) return (char) 0;
        if (rt == short.class) return (short) 0;
        if (rt == int.class) return 0;
        if (rt == long.class) return 0L;
        if (rt == float.class) return 0f;
        if (rt == double.class) return 0d;
        return null;
    }
}