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

import java.util.Set;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.request.ChunkFetchRequest;
import org.apache.spark.network.protocol.request.ClientRequest;
import org.apache.spark.network.protocol.request.RpcRequest;
import org.apache.spark.network.protocol.response.ChunkFetchFailure;
import org.apache.spark.network.protocol.response.ChunkFetchSuccess;
import org.apache.spark.network.protocol.response.RpcFailure;
import org.apache.spark.network.protocol.response.RpcResponse;

/**
 * A handler that processes requests from clients and writes chunk data back. Each handler keeps
 * track of which streams have been fetched via this channel, in order to clean them up if the
 * channel is terminated (see #channelUnregistered).
 *
 * The messages should have been processed by the pipeline setup by {@link SluiceServer}.
 */
public class SluiceServerHandler extends SimpleChannelInboundHandler<ClientRequest> {
  private final Logger logger = LoggerFactory.getLogger(SluiceServerHandler.class);

  /** Returns each chunk part of a stream. */
  private final StreamManager streamManager;

  /** Handles all RPC messages. */
  private final RpcHandler rpcHandler;

  /** List of all stream ids that have been read on this handler, used for cleanup. */
  private final Set<Long> streamIds;

  public SluiceServerHandler(StreamManager streamManager, RpcHandler rpcHandler) {
    this.streamManager = streamManager;
    this.rpcHandler = rpcHandler;
    this.streamIds = Sets.newHashSet();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.error("Exception in connection from " + ctx.channel().remoteAddress(), cause);
    ctx.close();
    super.exceptionCaught(ctx, cause);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    super.channelUnregistered(ctx);
    // Inform the StreamManager that these streams will no longer be read from.
    for (long streamId : streamIds) {
      streamManager.connectionTerminated(streamId);
    }
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, ClientRequest request) {
    if (request instanceof ChunkFetchRequest) {
      processFetchRequest(ctx, (ChunkFetchRequest) request);
    } else if (request instanceof RpcRequest) {
      processRpcRequest(ctx, (RpcRequest) request);
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

  private void processFetchRequest(final ChannelHandlerContext ctx, final ChunkFetchRequest req) {
    final String client = ctx.channel().remoteAddress().toString();
    streamIds.add(req.streamChunkId.streamId);

    logger.trace("Received req from {} to fetch block {}", client, req.streamChunkId);

    ManagedBuffer buf;
    try {
      buf = streamManager.getChunk(req.streamChunkId.streamId, req.streamChunkId.chunkIndex);
    } catch (Exception e) {
      logger.error(String.format(
        "Error opening block %s for request from %s", req.streamChunkId, client), e);
      respond(ctx, new ChunkFetchFailure(req.streamChunkId, Throwables.getStackTraceAsString(e)));
      return;
    }

    respond(ctx, new ChunkFetchSuccess(req.streamChunkId, buf));
  }

  private void processRpcRequest(final ChannelHandlerContext ctx, final RpcRequest req) {
    try {
      rpcHandler.receive(req.message, new RpcResponseCallback() {
        @Override
        public void onSuccess(byte[] response) {
          respond(ctx, new RpcResponse(req.tag, response));
        }

        @Override
        public void onFailure(Throwable e) {
          respond(ctx, new RpcFailure(req.tag, Throwables.getStackTraceAsString(e)));
        }
      });
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() on RPC tag " + req.tag, e);
      respond(ctx, new RpcFailure(req.tag, Throwables.getStackTraceAsString(e)));
    }
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * it will be logged and the channel closed.
   */
  private void respond(final ChannelHandlerContext ctx, final Encodable result) {
    final String remoteAddress = ctx.channel().remoteAddress().toString();
    ctx.writeAndFlush(result).addListener(
      new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            logger.trace(String.format("Sent result %s to client %s", result, remoteAddress));
          } else {
            logger.error(String.format("Error sending result %s to %s; closing connection",
              result, remoteAddress), future.cause());
            ctx.close();
          }
        }
      }
    );
  }
}
