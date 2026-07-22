/*
 * ============================================================================
 * mlc_neb — NMS 反射辅助（中央化、对抗版本变化）
 * ============================================================================
 *
 * Paper 插件官方不通过 paper-api 暴露 NMS。 本插件为避免引入 paperweight
 * 开发包依赖，所有对 net.minecraft.* 与 craftbukkit 的访问集中在本类通过反射进行。
 *
 * 访问链（出站，用于获取玩家 Netty Channel）:
 *   org.bukkit.craftbukkit.<pkg>.entity.CraftPlayer.getHandle()
 *        → net.minecraft.server.level.ServerPlayer
 *        .connection    → net.minecraft.server.network.ServerGamePacketListenerImpl
 *        .connection    → net.minecraft.network.Connection   (公开 getConnection() 在多版本稳定)
 *        .channel       → io.netty.channel.Channel          (netty 运行时提供)
 *
 * craftbukkit 包前缀（v<大版本>_R<次版本>）跨版本变化，故用
 * Bukkit.getServer().getClass().getPackage().getName() 动态取得。
 *
 * 所有 minecraft 服务端类在 Paper 1.21.11 上使用 Mojang 官方映射（mojmap）类名，
 * 成员名采用 mojmap 名（非混淆）。Paper 自 1.20.5+ 重打包后服务端以 mojmap 运行，
 * 即反射可直接用 minecraft.network.protocol.Packet 等 mojmap 全限定名。
 * ============================================================================
 */

package org.mlc.mlc_neb.network;

import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 集中所有 NMS / craftbukkit 反射，向其它类提供稳定的调用句柄。
 *
 * <p>初始化时一次性解析并缓存 {@link MethodHandle}；调用失败抛出受检
 * {@link ReflectException} 以便上层决定降级。</p>
 */
public final class NmsReflect {

    private static final Logger logger = Logger.getLogger("mlc_neb");

    // ---- 缓存句柄 ----
    private static volatile boolean initialized = false;
    private static volatile boolean available = false;

    private static Class<?> craftPlayerClass;
    private static MethodHandle craftGetHandle;        // CraftPlayer.getHandle() -> ServerPlayer

    private static MethodHandle serverPlayerConnection; // field ServerPlayer.connection -> ServerGamePacketListenerImpl
    private static MethodHandle listenerConnection;   // ServerGamePacketListenerImpl.getConnection() OR field -> Connection
    private static MethodHandle connectionChannel;    // field Connection.channel -> io.netty Channel

    // ---- Minecraft 协议层（用于 flush 时编码每个子包）----
    private static Class<?> connectionClass;
    private static Class<?> packetClass;               // net.minecraft.network.protocol.Packet
    private static MethodHandle packetTypeId;          // Packet.type() -> PacketType<?>
    private static MethodHandle packetTypeIdIdentifier; // PacketType.id() -> ResourceLocation

    private static MethodHandle connectionSend3;       // Connection.send(Packet, ChannelFutureListener, boolean)
    private static MethodHandle connectionSend1;       // Connection.send(Packet)  fallback

    // 用于构造自定义 payload 并包进 ClientboundCustomPayloadPacket 发送:
    private static Class<?> cppInterface;             // net.minecraft.network.protocol.common.custom.CustomPacketPayload
    private static Class<?> cppTypeClass;             // CustomPacketPayload.Type
    private static Class<?> resourceLocationClass;    // net.minecraft.resources.ResourceLocation
    private static MethodHandle rlFromNsPath;         // ResourceLocation.fromNamespaceAndPath(String,String) 或 new(ns,path)
    private static Class<?> customPayloadPacketClass;  // net.minecraft.network.protocol.common.ClientboundCustomPayloadPacket
    private static Class<?> streamCodecInterface;      // net.minecraft.network.codec.StreamCodec
    private static Class<?> friendlyByteBufClass;      // net.minecraft.network.FriendlyByteBuf

    private NmsReflect() {}

    /** 初始化反射链。失败则 available=false，调用方应降级为原版直通。 */
    public static synchronized void init() {
        if (initialized) return;
        initialized = true;
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            String cbPkg = Bukkit.getServer().getClass().getPackage().getName();
            // cbPkg == org.bukkit.craftbukkit.v1_21_R3
            craftPlayerClass = Class.forName(cbPkg + ".entity.CraftPlayer");
            Method getHandle = craftPlayerClass.getMethod("getHandle");
            craftGetHandle = lookup.unreflect(getHandle);

            Class<?> serverPlayerClass = getHandle.getReturnType(); // ServerPlayer
            Field connField = findField(serverPlayerClass, "connection");
            serverPlayerConnection = lookup.unreflectGetter(getAccessible(connField));

            Class<?> listenerClass = connField.getType(); // ServerGamePacketListenerImpl
            connectionClass = tryClass(
                    "net.minecraft.network.Connection",
                    "net.minecraft.network.NetworkManager");

            MethodHandle listenerToConn;
            // 优先公开方法 getConnection()
            Method mGetConn = findMethod(listenerClass, "getConnection");
            if (mGetConn != null && connectionClass.isAssignableFrom(mGetConn.getReturnType())) {
                listenerToConn = lookup.unreflect(mGetConn);
            } else {
                Field fConnInListener = findField(listenerClass, "connection");
                if (fConnInListener == null) fConnInListener = findField(listenerClass, "conn");
                listenerToConn = lookup.unreflectGetter(getAccessible(fConnInListener));
            }
            listenerConnection = listenerToConn;

            Field channelField = findField(connectionClass, "channel");
            connectionChannel = lookup.unreflectGetter(getAccessible(channelField));

            // Packet / PacketType / ResourceLocation
            Class<?> connectionProtocolClass = tryClass(
                    "net.minecraft.network.ConnectionProtocol",
                    "net.minecraft.network.protocol.ConnectionProtocol");
            packetClass = tryClass(
                    "net.minecraft.network.protocol.Packet");
            // Packet.type():PacketType<?>. Paper 1.21 mojmap 下稳定存在。
            Method mType = findMethod(packetClass, "type");
            packetTypeId = lookup.unreflect(mType);

            // PacketType.id():ResourceLocation。 这是黑名单与 NEB line-format prefix 的统一来源。
            // 原版 Author 错误地不拿类型,导致黑名单失效 + 客户端解包失败。
            Class<?> packetTypeClass = mType.getReturnType(); // PacketType<?>
            Method mTypeId = findMethod(packetTypeClass, "id");
            packetTypeIdIdentifier = lookup.unreflect(mTypeId);

            resourceLocationClass = tryClass(
                    "net.minecraft.resources.ResourceLocation",
                    "net.minecraft.resources.Identifier");

            // ResourceLocation.fromNamespaceAndPath建于 1.21+; 老版本用 new(String,String)
            MethodHandle rlCtor = null;
            try {
                Method m = resourceLocationClass.getMethod("fromNamespaceAndPath", String.class, String.class);
                rlCtor = lookup.unreflect(m);
            } catch (NoSuchMethodException notFound) {
                // 构造器 new(String namespace, String path)
                rlCtor = lookup.findConstructor(resourceLocationClass,
                        MethodType.methodType(void.class, String.class, String.class));
            }
            rlFromNsPath = rlCtor;

            // CustomPayload 接口 + Type + ClientboundCustomPayloadPacket
            cppInterface = tryClass(
                    "net.minecraft.network.protocol.common.custom.CustomPacketPayload");
            cppTypeClass = tryClass(
                    "net.minecraft.network.protocol.common.custom.CustomPacketPayload$Type");
            customPayloadPacketClass = tryClass(
                    "net.minecraft.network.protocol.common.ClientboundCustomPayloadPacket");
            streamCodecInterface = tryClass(
                    "net.minecraft.network.codec.StreamCodec");
            friendlyByteBufClass = tryClass(
                    "net.minecraft.network.FriendlyByteBuf");

            // Connection.send 存在 3 参重载，回退 1 参
            MethodHandle send3 = null;
            Class<?> futureListenerClass = tryClass(
                    "io.netty.channel.ChannelFutureListener",
                    "net.minecraft.network.ChannelFutureListener");
            try {
                Method m3 = findMethod(connectionClass, "send", packetClass, futureListenerClass, boolean.class);
                if (m3 != null && m3.getParameterTypes().length == 3) {
                    send3 = lookup.unreflect(m3);
                }
            } catch (Exception ignored) {}
            connectionSend3 = send3;
            Method send1 = findMethod(connectionClass, "send", packetClass);
            connectionSend1 = (send1 != null) ? lookup.unreflect(send1) : null;

            available = true;
            logger.info("NMS 反射初始化成功 (cb=" + cbPkg + ", Connection="
                    + connectionClass.getName() + ", Packet=" + packetClass.getName() + ").");
        } catch (Throwable t) {
            available = false;
            logger.log(Level.SEVERE,
                    "NMS 反射初始化失败 — NEB 聚合将不可用（原版路径不受影响）", t);
        }
    }

    public static boolean isAvailable() { return available; }
    public static Class<?> getPacketClass() { return packetClass; }
    public static Class<?> getConnectionClass() { return connectionClass; }
    public static Class<?> getCustomPayloadPacketClass() { return customPayloadPacketClass; }
    public static Class<?> getCustomPayloadInterface() { return cppInterface; }
    public static Class<?> getCustomPayloadTypeClass() { return cppTypeClass; }
    public static Class<?> getResourceLocationClass() { return resourceLocationClass; }
    public static Class<?> getStreamCodecInterface() { return streamCodecInterface; }
    public static Class<?> getFriendlyByteBufClass() { return friendlyByteBufClass; }

    /** 获取玩家连接的 netty Channel，失败返回 null。 */
    public static io.netty.channel.Channel getChannel(Player player) {
        if (!available) return null;
        try {
            Object craftPlayer = craftPlayerClass.cast(player);
            Object serverPlayer = craftGetHandle.invoke(craftPlayer);
            Object listener = serverPlayerConnection.invoke(serverPlayer);
            if (listener == null) return null;
            Object connection = listenerConnection.invoke(listener);
            if (connection == null) return null;
            return (io.netty.channel.Channel) connectionChannel.invoke(connection);
        } catch (Throwable t) {
            logger.log(Level.FINE, "获取玩家 " + player.getName() + " 的 Channel 失败", t);
            return null;
        }
    }

    /** 获取玩家连接对应的 NMS Connection 对象，失败返回 null。 */
    public static Object getConnection(Player player) {
        if (!available) return null;
        try {
            Object craftPlayer = craftPlayerClass.cast(player);
            Object serverPlayer = craftGetHandle.invoke(craftPlayer);
            Object listener = serverPlayerConnection.invoke(serverPlayer);
            if (listener == null) return null;
            return listenerConnection.invoke(listener);
        } catch (Throwable t) {
            logger.log(Level.FINE, "获取玩家 " + player.getName() + " 的 Connection 失败", t);
            return null;
        }
    }

    /** 通过传入了的 Connection 获取其 channel（用于已持有 connection 的路径）。 */
    public static io.netty.channel.Channel getChannelOfConnection(Object connection) {
        if (!available || connection == null) return null;
        try {
            return (io.netty.channel.Channel) connectionChannel.invoke(connection);
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * 返回 packet.type().id().toString() 即 "namespace:path" 形式的类型标识。
     * 失败返回 null（调用方应放行该包，不做聚合）。
     */
    public static String getPacketTypeId(Object packet) {
        if (!available || packet == null) return null;
        try {
            Object type = packetTypeId.invoke(packet);
            Object id = packetTypeIdIdentifier.invoke(type);
            return id == null ? null : id.toString();
        } catch (Throwable t) {
            return null;
        }
    }

    /** 构造 ResourceLocation(ns, path)。 */
    public static Object newResourceLocation(String namespace, String path) throws Throwable {
        return rlFromNsPath.invoke(namespace, path);
    }

    /**
     * 通过 Connection.send 发送一个已是 NMS Packet 的聚合包。
     * 优先 3 参重载（with listener=null, flush=true），回退 1 参。
     */
    public static void sendPacket(Object connection, Object packet) throws Throwable {
        if (connectionSend3 != null) {
            connectionSend3.invoke(connection, packet, null, true);
        } else if (connectionSend1 != null) {
            connectionSend1.invoke(connection, packet);
        } else {
            throw new IllegalStateException("Connection.send 不可用");
        }
    }

    // ========================================================================
    //  反射工具
    // ========================================================================

    private static Field findField(Class<?> clazz, String... names) {
        Class<?> c = clazz;
        while (c != null) {
            for (String n : names) {
                try {
                    return c.getDeclaredField(n);
                } catch (NoSuchFieldException ignored) {}
            }
            c = c.getSuperclass();
        }
        return null;
    }

    private static Method findMethod(Class<?> clazz, String name, Class<?>... paramTypes) {
        Class<?> c = clazz;
        while (c != null) {
            try {
                return c.getDeclaredMethod(name, paramTypes);
            } catch (NoSuchMethodException ignored) {}
            // 若未指定参数类型，遍历同名所有方法取第一个
            if (paramTypes == null || paramTypes.length == 0) {
                for (Method m : c.getDeclaredMethods()) {
                    if (m.getName().equals(name)) return m;
                }
            }
            c = c.getSuperclass();
        }
        return null;
    }

    private static Field getAccessible(Field f) {
        if (f == null) return null;
        try { f.setAccessible(true); } catch (Exception ignored) {}
        return f;
    }

    /** 按候选全限定名依次尝试加载，第一个成功即返回。 */
    private static Class<?> tryClass(String... names) throws ClassNotFoundException {
        for (String n : names) {
            try {
                return Class.forName(n);
            } catch (ClassNotFoundException ignored) {}
        }
        throw new ClassNotFoundException(names[0]);
    }

    /** 反射操作失败异常。 */
    public static final class ReflectException extends RuntimeException {
        public ReflectException(String msg, Throwable cause) { super(msg, cause); }
    }
}