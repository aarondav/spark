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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.SluiceClient;
import org.apache.spark.network.client.SluiceResponseHandler;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.RequestMessage;
import org.apache.spark.network.protocol.ResponseMessage;
import org.apache.spark.network.util.NettyUtils;

/**
 * A handler that processes requests from clients and writes chunk data back. Each handler keeps
 * track of which streams have been fetched via this channel, in order to clean them up if the
 * channel is terminated (see #channelUnregistered).
 *
 * The messages should have been processed by the pipeline setup by
 * {@link org.apache.spark.network.server.SluiceServer}.
 */
public class MessageDispatcher extends SimpleChannelInboundHandler<Message> {
  private final Logger logger = LoggerFactory.getLogger(MessageDispatcher.class);

  private final SluiceClient client;
  private final SluiceResponseHandler responseHandler;
  private final SluiceRequestHandler requestHandler;

  public MessageDispatcher(
      SluiceClient client,
      SluiceResponseHandler responseHandler,
      SluiceRequestHandler requestHandler) {
    this.client = client;
    this.responseHandler = responseHandler;
    this.requestHandler = requestHandler;
  }

  public SluiceClient getClient() {
    return client;
  }

  public SluiceResponseHandler getResponseHandler() {
    return responseHandler;
  }

  public SluiceRequestHandler getRequestHandler() {
    return requestHandler;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.error("Exception in connection from " + NettyUtils.getRemoteAddress(ctx.channel()),
      cause);
    requestHandler.exceptionCaught(cause);
    responseHandler.exceptionCaught(cause);
    ctx.close();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    requestHandler.channelUnregistered();
    responseHandler.channelUnregistered();
    super.channelUnregistered(ctx);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Message request) {
    if (request instanceof RequestMessage) {
      requestHandler.handle((RequestMessage) request);
    } else {
      responseHandler.handle((ResponseMessage) request);
    }
  }
}
