/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.netty.example.redis;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * 这段代码是一个使用Netty框架实现的Redis客户端程序的主函数。主函数中的代码主要完成以下几个步骤：
 *
 * 创建一个NioEventLoopGroup对象，用于处理网络事件。
 *
 * 创建一个Bootstrap对象，用于启动客户端。
 *
 * 设置Bootstrap对象的参数，包括使用NioSocketChannel作为通道类型、设置ChannelInitializer对象等。
 *
 * 在ChannelInitializer对象的initChannel()方法中，设置ChannelPipeline对象，添加RedisDecoder、RedisBulkStringAggregator、RedisArrayAggregator、RedisEncoder和RedisClientHandler等ChannelHandler。
 *
 * 调用Bootstrap对象的connect()方法，连接到Redis服务器。
 *
 * 调用sync()方法，等待连接完成。
 *
 * 获取Channel对象，用于发送和接收数据。
 * <p>
 * 在这段代码中，RedisDecoder、RedisBulkStringAggregator、RedisArrayAggregator、RedisEncoder和RedisClientHandler都是自定义的ChannelHandler，用于处理Redis协议的编解码和业务逻辑。其中，RedisDecoder用于将字节数据解码为Redis消息对象，RedisBulkStringAggregator和RedisArrayAggregator用于将多个Redis消息合并为一个完整的消息，RedisEncoder用于将Redis消息对象编码为字节数据，RedisClientHandler用于处理Redis客户端的业务逻辑。

 * 通过使用Netty框架，可以方便地实现各种类型的网络应用程序，包括客户端和服务器端。Netty框架提供了丰富的组件和API，可以大大简化网络编程的复杂度，提高开发效率和程序性能。
 */
public class RedisClient {
    private static final String HOST = System.getProperty("host", "127.0.0.1");
    private static final int PORT = Integer.parseInt(System.getProperty("port", "6379"));

    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) throws Exception {
                     ChannelPipeline p = ch.pipeline();
                     p.addLast(new RedisDecoder());
                     /**
                      * RedisBulkStringAggregator用于将多个Bulk String类型的消息合并为一个完整的消息。
                      * 在Redis协议中，Bulk String类型的消息是以"$"开头，后面跟着一个数字表示消息的长度，再后面跟着实际的消息内容。
                      * 由于Bulk String类型的消息可能会被拆分成多个数据包进行传输，因此需要使用RedisBulkStringAggregator将多个数据包合并为一个完整的消息。
                      */
                     p.addLast(new RedisBulkStringAggregator());
                     /**
                      * RedisArrayAggregator用于将多个Array类型的消息合并为一个完整的消息。
                      * 在Redis协议中，Array类型的消息是以"*"开头，后面跟着一个数字表示消息中包含的元素个数，再后面跟着实际的消息内容。
                      * 由于Array类型的消息也可能会被拆分成多个数据包进行传输，因此需要使用RedisArrayAggregator将多个数据包合并为一个完整的消息。
                      */
                     p.addLast(new RedisArrayAggregator());
                     p.addLast(new RedisEncoder());
                     p.addLast(new RedisClientHandler());
                 }
             });

            // Start the connection attempt.
            Channel ch = b.connect(HOST, PORT).sync().channel();

            // Read commands from the stdin.
            System.out.println("Enter Redis commands (quit to end)");
            ChannelFuture lastWriteFuture = null;
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            for (;;) {
                final String input = in.readLine();
                final String line = input != null ? input.trim() : null;
                if (line == null || "quit".equalsIgnoreCase(line)) { // EOF or "quit"
                    ch.close().sync();
                    break;
                } else if (line.isEmpty()) { // skip `enter` or `enter` with spaces.
                    continue;
                }
                // Sends the received line to the server.
                lastWriteFuture = ch.writeAndFlush(line);
                lastWriteFuture.addListener(new GenericFutureListener<ChannelFuture>() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            System.err.print("write failed: ");
                            future.cause().printStackTrace(System.err);
                        }
                    }
                });
            }

            // Wait until all messages are flushed before closing the channel.
            if (lastWriteFuture != null) {
                lastWriteFuture.sync();
            }
        } finally {
            group.shutdownGracefully();
        }
    }
}
