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

package org.apache.spark.network.netty

import java.nio.ByteBuffer
import java.util

import org.apache.spark.Logging
import org.apache.spark.network.BlockFetchingListener
import org.apache.spark.serializer.Serializer
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, ChunkReceivedCallback, SluiceClient}
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils

class NettyBlockFetcher(
    serializer: Serializer,
    client: SluiceClient,
    blockIds: Seq[String],
    listener: BlockFetchingListener)
  extends Logging {

  require(blockIds.nonEmpty)

  val ser = serializer.newInstance()

  var streamHandle: ShuffleStreamHandle = _

  val chunkCallback = new ChunkReceivedCallback {
    def onSuccess(chunkIndex: Int, buffer: ManagedBuffer): Unit = Utils.logUncaughtExceptions {
      buffer.retain()
      listener.onBlockFetchSuccess(blockIds(chunkIndex), buffer)
    }

    def onFailure(chunkIndex: Int, e: Throwable): Unit = {
      // Assume we've lost everything from chunkIndex onwards.
      blockIds.drop(chunkIndex).foreach { blockId =>
        listener.onBlockFetchFailure(blockId, e);
      }
    }
  }

  client.sendRpc(ser.serialize(OpenBlocks(blockIds.map(BlockId.apply))).array(),
    new RpcResponseCallback {
      override def onSuccess(response: Array[Byte]): Unit = {
        try {
          streamHandle = ser.deserialize(ByteBuffer.wrap(response))
          logInfo(s"Successfully opened block set: $streamHandle! Preparing to fetch chunks.")

          for (i <- 0 until streamHandle.numChunks) {
            client.fetchChunk(streamHandle.streamId, i, chunkCallback)
          }
        } catch {
          case e: Exception =>
            logError("Failed while starting block fetches", e)
            blockIds.foreach(listener.onBlockFetchFailure(_, e))
        }
      }

      override def onFailure(e: Throwable): Unit = {
        logError("Failed while starting block fetches")
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
      }
    })
}
