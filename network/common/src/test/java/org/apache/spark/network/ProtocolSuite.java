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

package org.apache.spark.network;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.request.ChunkFetchRequest;
import org.apache.spark.network.protocol.response.ChunkFetchFailure;
import org.apache.spark.network.protocol.response.ChunkFetchSuccess;
import org.apache.spark.network.protocol.response.MessageDecoder;
import org.apache.spark.network.protocol.response.MessageEncoder;
import org.apache.spark.network.util.NettyUtils;

public class ProtocolSuite {
  private void testServerToClient(Message msg) {
    EmbeddedChannel serverChannel = new EmbeddedChannel(new MessageEncoder());
    serverChannel.writeOutbound(msg);

    EmbeddedChannel clientChannel = new EmbeddedChannel(
        NettyUtils.createFrameDecoder(), new MessageDecoder());

    while (!serverChannel.outboundMessages().isEmpty()) {
      clientChannel.writeInbound(serverChannel.readOutbound());
    }

    assertEquals(1, clientChannel.inboundMessages().size());
    assertEquals(msg, clientChannel.readInbound());
  }

  private void testClientToServer(Message msg) {
    EmbeddedChannel clientChannel = new EmbeddedChannel(new MessageEncoder());
    clientChannel.writeOutbound(msg);

    EmbeddedChannel serverChannel = new EmbeddedChannel(
        NettyUtils.createFrameDecoder(), new MessageDecoder());

    while (!clientChannel.outboundMessages().isEmpty()) {
      serverChannel.writeInbound(clientChannel.readOutbound());
    }

    assertEquals(1, serverChannel.inboundMessages().size());
    assertEquals(msg, serverChannel.readInbound());
  }

  @Test
  public void s2cChunkFetchSuccess() {
    testServerToClient(new ChunkFetchSuccess(new StreamChunkId(1, 2), new TestManagedBuffer(10)));
    testServerToClient(new ChunkFetchSuccess(new StreamChunkId(1, 2), new TestManagedBuffer(0)));
  }

  @Test
  public void s2cBlockFetchFailure() {
    testServerToClient(new ChunkFetchFailure(new StreamChunkId(1, 2), "this is an error"));
    testServerToClient(new ChunkFetchFailure(new StreamChunkId(1, 2), ""));
  }

  @Test
  public void c2sChunkFetchRequest() {
    testClientToServer(new ChunkFetchRequest(new StreamChunkId(1, 2)));
  }
}
