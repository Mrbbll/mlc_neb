/*
 * ============================================================================
 * mlc_neb — Zstd 压缩器
 * ============================================================================
 *
 * 封装 zstd-jni 库，为 NEB 数据包聚合管道提供 Zstd 压缩/解压。
 *
 * 相比原版 Minecraft 的 DEFLATE/zlib，Zstd 的优势:
 *   1. 同等速度下更好的压缩率
 *   2. 解压速度更快（对客户端重要）
 *   3. 支持压缩上下文复用 — 上下文随时间"学习"数据模式，
 *      逐步提高压缩率
 *   4. magicless 模式 — 省略 4 字节 Zstd 魔术头，
 *      NEB 使用自己的帧格式
 *
 * 每个连接拥有独立的 ZstdCompressor 实例。
 * 通过 "context-exclusion" 配置可以禁用特定玩家的上下文复用。
 *
 * 依赖: com.github.luben:zstd-jni (通过 shadowJar 打包)
 * ============================================================================
 */

package org.mlc.mlc_neb.compression;

import com.github.luben.zstd.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

/**
 * 每连接的 Zstd 压缩器，支持可选的上下文复用。
 *
 * <h4>上下文复用</h4>
 * <p>当 useContext=true（默认）时，压缩上下文在多次刷新间复用。
 * Zstd 内部会维护一个已见数据模式的字典，随时间提高压缩率。
 * 前几次刷新压缩率约 50%，几秒后通常提升到 15-30%。</p>
 *
 * <h4>上下文窗口级别</h4>
 * <p>windowLog 控制 Zstd 压缩窗口的内存大小:
 * <ul>
 *   <li>21 = 2MB   |   22 = 4MB   |   23 = 8MB（默认）</li>
 *   <li>24 = 16MB  |   25 = 32MB</li>
 * </ul>
 * 更大窗口 = 更好的压缩率但多用内存。</p>
 *
 * <h4>线程安全</h4>
 * <p>本类非线程安全。每连接一个实例，同一连接的压缩/解压
 * 始终在服务端线程上执行。</p>
 */
public class ZstdCompressor implements AutoCloseable {

    private final int compressionLevel;
    private final int windowLog;
    private final boolean useContext;

    /** 压缩上下文（useContext=true 时复用）。 */
    private ZstdCompressCtx compressCtx;

    /** 解压上下文。 */
    private ZstdDecompressCtx decompressCtx;

    /** 是否已关闭。 */
    private volatile boolean closed = false;

    // ========================================================================
    //  构造
    // ========================================================================

    /**
     * 创建新的 Zstd 压缩器。
     *
     * @param compressionLevel 压缩级别 (1-22，默认 3)。
     *                         级别 3 提供良好的速度/压缩率平衡。
     * @param windowLog        上下文窗口级别 (21-25，默认 23 = 8MB)
     * @param useContext       是否在多次刷新间复用压缩上下文
     */
    public ZstdCompressor(int compressionLevel, int windowLog, boolean useContext) {
        this.compressionLevel = clamp(compressionLevel, 1, 22);
        this.windowLog = clamp(windowLog, 21, 25);
        this.useContext = useContext;

        initContexts();
    }

    /**
     * 初始化 Zstd 压缩和解压上下文。
     */
    private void initContexts() {
        this.compressCtx = new ZstdCompressCtx();
        compressCtx.setLevel(compressionLevel);
        compressCtx.setContentSize(false);  // 自己跟踪大小
        compressCtx.setMagicless(true);     // NEB 自己管理帧格式
        compressCtx.setWindowLog(windowLog);

        this.decompressCtx = new ZstdDecompressCtx();
        decompressCtx.setMagicless(true);
    }

    // ========================================================================
    //  压缩
    // ========================================================================

    /**
     * 压缩 Netty ByteBuf 的内容。
     *
     * <p>若启用上下文复用，使用流式压缩
     * ({@link ZstdCompressCtx#compressDirectByteBufferStream})，
     * Zstd 可以在多次调用间维护内部字典。
     * 否则使用简单的一次性压缩。</p>
     *
     * @param raw 包含未压缩数据的缓冲区
     * @return 压缩后的字节数组
     */
    public byte[] compress(ByteBuf raw) {
        if (closed) {
            throw new IllegalStateException("ZstdCompressor 已关闭");
        }

        // 将 Netty ByteBuf 转为 NIO ByteBuffer
        ByteBuffer rawBuffer;
        if (raw.nioBufferCount() > 0) {
            rawBuffer = raw.nioBuffer();
        } else {
            byte[] bytes = new byte[raw.readableBytes()];
            raw.getBytes(raw.readerIndex(), bytes);
            rawBuffer = ByteBuffer.allocateDirect(bytes.length);
            rawBuffer.put(bytes);
            rawBuffer.flip();
        }

        ByteBuffer compressed;
        if (useContext) {
            // 流式压缩(上下文复用): EndDirective.FLUSH 表示"刷出一个完整帧并保留上下文字典"。
            // 客户端 ZstdHelper.Context 同样用流式 + magicless,两端必须一致才能解压。
            // 注意:不使用 EndDirective.END,否则上下文字典会被重置,失去复用。
            int maxDstSize = (int) Zstd.compressBound(rawBuffer.remaining());
            ByteBuffer dst = ByteBuffer.allocateDirect(maxDstSize);
            compressCtx.compressDirectByteBufferStream(
                    dst, rawBuffer, EndDirective.FLUSH);
            dst.flip();
            compressed = dst;
        } else {
            // 一次性压缩(无上下文共享): 等价客户端 useContext=false 分支。
            compressed = compressCtx.compress(rawBuffer);
        }

        byte[] result = new byte[compressed.remaining()];
        compressed.get(result);

        return result;
    }

    /**
     * 压缩原始字节数组。非 Netty 场景的便捷重载。
     */
    public byte[] compress(byte[] raw) {
        if (closed) {
            throw new IllegalStateException("ZstdCompressor 已关闭");
        }

        ByteBuffer rawBuffer = ByteBuffer.allocateDirect(raw.length);
        rawBuffer.put(raw);
        rawBuffer.flip();

        ByteBuffer compressed;
        if (useContext) {
            int maxDstSize = (int) Zstd.compressBound(rawBuffer.remaining());
            ByteBuffer dst = ByteBuffer.allocateDirect(maxDstSize);
            compressCtx.compressDirectByteBufferStream(
                    dst, rawBuffer, EndDirective.FLUSH);
            dst.flip();
            compressed = dst;
        } else {
            compressed = compressCtx.compress(rawBuffer);
        }

        byte[] result = new byte[compressed.remaining()];
        compressed.get(result);
        return result;
    }

    // ========================================================================
    //  解压
    // ========================================================================

    /**
     * 将压缩的字节数组解压为 Netty ByteBuf。
     *
     * @param compressed   压缩数据
     * @param originalSize 预期解压后的大小
     * @return 包含解压数据的 Netty ByteBuf
     */
    public ByteBuf decompress(byte[] compressed, int originalSize) {
        if (closed) {
            throw new IllegalStateException("ZstdCompressor 已关闭");
        }

        ByteBuffer compressedBuffer = ByteBuffer.allocateDirect(compressed.length);
        compressedBuffer.put(compressed);
        compressedBuffer.flip();

        ByteBuffer dst = ByteBuffer.allocateDirect(originalSize);
        decompressCtx.decompressDirectByteBufferStream(dst, compressedBuffer);
        dst.flip();

        byte[] result = new byte[dst.remaining()];
        dst.get(result);

        return Unpooled.wrappedBuffer(result);
    }

    // ========================================================================
    //  清理
    // ========================================================================

    /**
     * 释放原生 Zstd 压缩和解压上下文。
     * 必须在连接关闭时调用以防内存泄漏。
     */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                if (compressCtx != null) {
                    compressCtx.close();
                    compressCtx = null;
                }
                if (decompressCtx != null) {
                    decompressCtx.close();
                    decompressCtx = null;
                }
            } catch (Exception ignored) {
                // 尽力清理
            }
        }
    }

    // ========================================================================
    //  工具
    // ========================================================================

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    @Override
    public String toString() {
        return "ZstdCompressor{level=" + compressionLevel
                + ", windowLog=" + windowLog
                + ", useContext=" + useContext
                + ", closed=" + closed + "}";
    }
}
