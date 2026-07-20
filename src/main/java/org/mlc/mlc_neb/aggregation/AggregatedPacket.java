/*
 * ============================================================================
 * mlc_neb — 聚合数据包（子包单元）
 * ============================================================================
 *
 * 代表一个被拦截后等待聚合发送的 Minecraft 数据包。
 *
 * 每个聚合子包存储:
 *   - 数据包类型标识（如 "minecraft:level_chunk_with_light"）
 *   - 已编码的原始字节数据
 *
 * 刷新时所有缓冲的 AggregatedPacket 被拼接为一个 ByteBuf，
 * 经 Zstd 压缩后以一个 "neb:packet_aggregation_packet" 发出。
 *
 * ======== 子包线格式（与 NEB 客户端模组兼容）========
 *
 * ┌── X bytes ──┬── Y bytes ────────┬── Z bytes ──┬── W bytes ──┐
 * │ namespace长  │ namespace(UTF-8)  │ path长度     │ path(UTF-8) │
 * │ (varint)    │                   │ (varint)    │             │
 * └─────────────┴───────────────────┴─────────────┴─────────────┘
 * ┌────────── M bytes ────────┬────────── N bytes ─────────┐
 * │ payload长度 (varint)      │ payload字节                 │
 * └──────────────────────────┴────────────────────────────┘
 *
 * 类型标识使用 Minecraft 标准 Identifier 编码方式（writeIdentifier），
 * 客户端 NEB 模组的 CustomPacketPrefixHelper.read() 可以正确解析。
 * ============================================================================
 */

package org.mlc.mlc_neb.aggregation;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * 单个待聚合的 Minecraft 数据包。
 *
 * <p>持有包类型标识和已序列化的字节数据。
 * 在刷新时所有实例被批量拼接、压缩后发出。</p>
 *
 * <p>线程安全: 在 Netty 事件循环中创建和消费，无需额外同步。</p>
 */
public class AggregatedPacket {

    /** 数据包类型标识，如 "minecraft:level_chunk_with_light"。 */
    private final String typeId;

    /** 已序列化的原始 Minecraft 数据包字节（含包ID varint头）。 */
    private final byte[] data;

    /**
     * 创建一个新的聚合子包。
     *
     * @param typeId 包类型标识（格式: "namespace:path"）
     * @param data   已编码的包数据字节
     */
    public AggregatedPacket(String typeId, byte[] data) {
        this.typeId = typeId;
        this.data = data;
    }

    // ========================================================================
    //  序列化 — 与 NEB 客户端模组兼容的格式
    // ========================================================================

    /**
     * 将本子包编码写入 ByteBuf。
     *
     * <p>线格式（与客户端 NEB 模组 CustomPacketPrefixHelper 兼容）:
     * <pre>
     *   [varint: namespace字节长度] [utf8: namespace]
     *   [varint: path字节长度]      [utf8: path]
     *   [varint: payload字节长度]    [bytes: payload]
     * </pre>
     *
     * <p>这等价于 Minecraft 的 {@code FriendlyByteBuf.writeIdentifier()}
     * 后跟 VarInt 长度前缀 + 数据负载。</p>
     *
     * @param buf 目标缓冲区
     */
    public void writeTo(ByteBuf buf) {
        // 拆分 "namespace:path"
        int colonIdx = typeId.indexOf(':');
        String namespace, path;
        if (colonIdx >= 0) {
            namespace = typeId.substring(0, colonIdx);
            path = typeId.substring(colonIdx + 1);
        } else {
            namespace = "minecraft";
            path = typeId;
        }

        byte[] nsBytes = namespace.getBytes(StandardCharsets.UTF_8);
        byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);

        // 写入 namespace 长度 + 内容
        writeVarInt(buf, nsBytes.length);
        buf.writeBytes(nsBytes);

        // 写入 path 长度 + 内容
        writeVarInt(buf, pathBytes.length);
        buf.writeBytes(pathBytes);

        // 写入 payload 长度 + 内容
        writeVarInt(buf, data.length);
        buf.writeBytes(data);
    }

    /**
     * 从 ByteBuf 中读取一个聚合子包。
     * 为 {@link #writeTo(ByteBuf)} 的逆操作。
     *
     * @param buf 源缓冲区
     * @return 反序列化的 AggregatedPacket
     */
    public static AggregatedPacket readFrom(ByteBuf buf) {
        // 读取 namespace
        int nsLen = readVarInt(buf);
        byte[] nsBytes = new byte[nsLen];
        buf.readBytes(nsBytes);
        String namespace = new String(nsBytes, StandardCharsets.UTF_8);

        // 读取 path
        int pathLen = readVarInt(buf);
        byte[] pathBytes = new byte[pathLen];
        buf.readBytes(pathBytes);
        String path = new String(pathBytes, StandardCharsets.UTF_8);

        String typeId = namespace + ":" + path;

        // 读取 payload
        int dataLen = readVarInt(buf);
        byte[] data = new byte[dataLen];
        buf.readBytes(data);

        return new AggregatedPacket(typeId, data);
    }

    // ========================================================================
    //  VarInt 编解码（与 Minecraft 协议一致: Protocol Buffer 变长编码）
    // ========================================================================

    /**
     * 写入 VarInt 到缓冲区。
     * 使用与 Minecraft VarInt 相同的编码: 每字节低7位为数据，最高位为继续标志。
     */
    public static void writeVarInt(ByteBuf buf, int value) {
        while ((value & ~0x7F) != 0) {
            buf.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf.writeByte(value & 0x7F);
    }

    /**
     * 从缓冲区读取 VarInt。
     * @return 解码后的整数值
     * @throws RuntimeException 如果 VarInt 格式错误
     */
    public static int readVarInt(ByteBuf buf) {
        int value = 0;
        int position = 0;
        byte currentByte;

        while (true) {
            currentByte = buf.readByte();
            value |= (currentByte & 0x7F) << (position * 7);
            if ((currentByte & 0x80) == 0) break;
            position++;
            if (position > 5) {
                throw new RuntimeException("VarInt 过大 (最多5字节，当前位置 " + position + ")");
            }
        }

        return value;
    }

    // ========================================================================
    //  Getter
    // ========================================================================

    public String getTypeId() { return typeId; }
    public byte[] getData() { return data; }

    /** 返回序列化后的估计大小（用于调试日志中的压缩率计算）。 */
    public int getSerializedSize() {
        int colonIdx = typeId.indexOf(':');
        String ns = colonIdx >= 0 ? typeId.substring(0, colonIdx) : "minecraft";
        String path = colonIdx >= 0 ? typeId.substring(colonIdx + 1) : typeId;
        // varint(nsLen) + ns + varint(pathLen) + path + varint(dataLen) + data
        return 2 + ns.length() + 2 + path.length() + 3 + data.length;
    }

    @Override
    public String toString() {
        return "AggregatedPacket{type='" + typeId + "', size=" + data.length + "}";
    }
}
