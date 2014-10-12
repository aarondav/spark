/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.server;

import java.io.Closeable;
import java.net.InetSocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.response.MessageDecoder;
import org.apache.spark.network.protocol.response.MessageEncoder;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.SluiceConfig;

/**
 * Server for the efficient, low-level streaming service.
 */
public class SluiceServer implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(SluiceServer.class);

  private final SluiceConfig conf;
  private final MessageDispatcherFactory dispatcherFactory;

  private ServerBootstrap bootstrap;
  private ChannelFuture channelFuture;
  private int port;

  public SluiceServer(SluiceConfig conf, MessageDispatcherFactory dispatcherFactory) {
    this.conf = conf;
    this.dispatcherFactory = dispatcherFactory;

    init();
  }

  public int getPort() { return port; }

  private void init() {

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    EventLoopGroup bossGroup =
        NettyUtils.createEventLoop(ioMode, conf.serverThreads(), "shuffle-server");
    EventLoopGroup workerGroup = bossGroup;

    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NettyUtils.getServerChannelClass(ioMode))
      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    if (conf.backLog() > 0) {
      bootstrap.option(ChannelOption.SO_BACKLOG, conf.backLog());
    }

    if (conf.receiveBuf() > 0) {
      bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf());
    }

    if (conf.sendBuf() > 0) {
      bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf());
    }


    // MessageDispatcher (one per channel) - SluiceClient, SluiceClientHandler, SluiceServerHandler
    // SluiceClientFactory (input: address)
    // SluiceServerPipelineFactory (input: Channel)

    // SluiceClient --- sendRPC --> SluiceServerHandler
    // SluiceServerHandler <-- sendRPC --- SluiceServer
    // SluiceClient --- sendRPC --> SluiceServer


    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {

      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
          .addLast("frameDecoder", NettyUtils.createFrameDecoder())
          .addLast("decoder", new MessageDecoder())
          .addLast("encoder", new MessageEncoder())
          // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
          // would require more logic to guarantee if this were not part of the same event loop.
          .addLast("handler", dispatcherFactory.createDispatcher(ch));
      }
    });

    channelFuture = bootstrap.bind(new InetSocketAddress(conf.serverPort()));
    channelFuture.syncUninterruptibly();

    port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
    logger.debug("Shuffle server started on port :" + port);
  }

  @Override
  public void close() {
    if (channelFuture != null) {
      channelFuture.channel().close().awaitUninterruptibly();
      channelFuture = null;
    }
    if (bootstrap != null && bootstrap.group() != null) {
      bootstrap.group().shutdownGracefully();
    }
    if (bootstrap != null && bootstrap.childGroup() != null) {
      bootstrap.childGroup().shutdownGracefully();
    }
    bootstrap = null;
  }

}
