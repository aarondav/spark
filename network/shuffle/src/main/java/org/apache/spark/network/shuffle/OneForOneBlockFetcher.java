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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.ShuffleMessages.ShuffleStreamHandle;
import org.apache.spark.network.util.JavaUtils;

public class OneForOneBlockFetcher {
  private final Logger logger = LoggerFactory.getLogger(OneForOneBlockFetcher.class);

  private final TransportClient client;
  private final String[] blockIds;
  private final BlockFetchingListener listener;
  private final ChunkReceivedCallback chunkCallback;

  private ShuffleStreamHandle streamHandle = null;

  public OneForOneBlockFetcher(
    TransportClient client,
    String[] blockIds,
    BlockFetchingListener listener) {
    if (blockIds.length == 0) {
      throw new IllegalArgumentException("Zero-sized blockIds array");
    }
    this.client = client;
    this.blockIds = blockIds;
    this.listener = listener;
    this.chunkCallback = new ChunkCallback();
  }

  /** Callback invoked on receipt of each chunk. We equate a single chunk to a single block. */
  private class ChunkCallback implements ChunkReceivedCallback {
    @Override
    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
      // On receipt of a chunk, pass it upwards as a block.
      listener.onBlockFetchSuccess(blockIds[chunkIndex], buffer);
    }

    @Override
    public void onFailure(int chunkIndex, Throwable e) {
      // On receipt of a failure, fail every block from chunkIndex onwards.
      String[] remainingBlockIds = Arrays.copyOfRange(blockIds, chunkIndex, blockIds.length);
      failRemainingBlocks(remainingBlockIds, e);
    }
  }

  /** Begins the fetching process, calling the listener with every block fetched. */
  public void start(Object openBlocksMessage) {
    client.sendRpc(JavaUtils.serialize(openBlocksMessage), new RpcResponseCallback() {
      @Override
      public void onSuccess(byte[] response) {
        try {
          streamHandle = JavaUtils.deserialize(response);
          logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);

          // Immediately request all chunks -- we expect that the total size of the request is
          // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
          for (int i = 0; i < streamHandle.numChunks; i++) {
            client.fetchChunk(streamHandle.streamId, i, chunkCallback);
          }
        } catch (Exception e) {
          logger.error("Failed while starting block fetches", e);
          failRemainingBlocks(blockIds, e);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        logger.error("Failed while starting block fetches", e);
        failRemainingBlocks(blockIds, e);
      }
    });
  }

  /** Invokes the "onBlockFetchFailure" callback for every listed block id. */
  private void failRemainingBlocks(String[] failedBlockIds, Throwable e) {
    for (String blockId : failedBlockIds) {
      try {
        listener.onBlockFetchFailure(blockId, e);
      } catch (Exception e2) {
        logger.error("Error in block fetch failure callback", e2);
      }
    }
  }
}
