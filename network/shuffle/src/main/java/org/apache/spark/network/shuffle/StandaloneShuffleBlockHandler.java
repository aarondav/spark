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

package org.apache.spark.network.shuffle;

import java.util.List;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.network.shuffle.ShuffleMessages.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.util.JavaUtils;

public class StandaloneShuffleBlockHandler implements RpcHandler {
  private final Logger logger = LoggerFactory.getLogger(StandaloneShuffleBlockHandler.class);

  private final OneForOneStreamManager streamManager;
  private final StandaloneShuffleBlockManager blockManager;

  public StandaloneShuffleBlockHandler() {
    this.streamManager = new OneForOneStreamManager();
    this.blockManager = new StandaloneShuffleBlockManager();
  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }

  @Override
  public void receive(TransportClient client, byte[] message, RpcResponseCallback callback) {
    Object msgObj = JavaUtils.deserialize(message);

    logger.info("Received message: " + msgObj);

    if (msgObj instanceof OpenShuffleBlocks) {
      OpenShuffleBlocks msg = (OpenShuffleBlocks) msgObj;
      List<ManagedBuffer> blocks = Lists.newArrayList();

      for (String blockId : msg.blockIds) {
        blocks.add(blockManager.getBlockData(msg.appId, msg.execId, blockId));
      }
      long streamId = streamManager.registerStream(blocks.iterator());
      logger.info("Registered streamId {} with {} buffers", streamId, msg.blockIds.length);
      callback.onSuccess(JavaUtils.serialize(
        new ShuffleStreamHandle(streamId, msg.blockIds.length)));

    } else if (msgObj instanceof RegisterExecutor) {
      RegisterExecutor msg = (RegisterExecutor) msgObj;
      blockManager.registerExecutor(msg.appId, msg.execId, msg.executorConfig);
      callback.onSuccess(new byte[0]);
    }
  }
}
