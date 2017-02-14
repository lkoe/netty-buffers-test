
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.CharsetUtil;

/**
 * Place breakpoints at io.netty.util.internal.PlatformDependent.incrementMemoryCounter(int) and
 * io.netty.util.internal.PlatformDependent.decrementMemoryCounter(int) to observe direct memory buffer allocations.
 *
 */
public class NettyBuffersTest {

    private static final int maxMessageLength = 1024 * 50; // 50 KB

    private static final int port = 13334;

    private static EventLoopGroup bossGroup;
    private static EventLoopGroup workerGroup;

    private static ServerInputHandler serverInputHandler = new ServerInputHandler();

    @ChannelHandler.Sharable
    private static class ServerInputHandler extends ChannelInboundHandlerAdapter {

        private AtomicInteger messagesReceived = new AtomicInteger(0);

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            System.out.println(msg);
            messagesReceived.incrementAndGet();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
            cause.printStackTrace();
            ctx.close();
        }

        public int getMessagesReceived() {
            return messagesReceived.get();
        }
    }

    @BeforeClass
    public static void startTcpServer() throws Exception {

        // System.setProperty("", value)

        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        System.out.println("Starting TCP Server");

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast("frameDecoder",
                                        new DelimiterBasedFrameDecoder(maxMessageLength, Delimiters.lineDelimiter()))
                                .addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8))
                                .addLast(serverInputHandler);
                    }
                }).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);

        b.bind(port).sync();
    }

    @AfterClass
    public static void stopTcpServer() throws Exception {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    @Test
    public void testSingleClient() throws Exception {

        createSocketAndSendSingleMessage("Dummy");
    }

    @Test
    public void testMultipleClients() throws Exception {

        for (int i = 0; i < 64; i++) {
            createSocketAndSendSingleMessage("Dummy" + i);
        }
    }

    private void createSocketAndSendSingleMessage(String message) throws UnknownHostException, IOException {

        int messagesBefore = serverInputHandler.getMessagesReceived();

        Socket socket = null;
        DataOutputStream out = null;
        try {
            socket = new Socket("127.0.0.1", port);
            out = new DataOutputStream(socket.getOutputStream());

            out.write((message + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();

            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> serverInputHandler.getMessagesReceived() == messagesBefore + 1);
        } finally {
            socket.close();
            out.close();
        }
    }
}
