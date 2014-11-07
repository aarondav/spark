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

import io.netty.buffer.{Unpooled, ByteBuf}
import org.apache.spark.network.protocol.Encodable
import org.apache.spark.network.shuffle.ExternalShuffleMessages.{ExternalShuffleMessage, OpenShuffleBlocks}
import org.apache.spark.network.util.JavaUtils

import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.{ExternalShuffleMessages, ShuffleStreamHandle}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, StorageLevel}

object NettyMessages extends ExternalShuffleMessages {
  /** Request to upload a block with a certain StorageLevel. Returns nothing (empty byte array). */
  case class UploadBlock(blockId: BlockId, blockData: Array[Byte], level: StorageLevel)
      extends ExternalShuffleMessage {
    private lazy val serializedVersion = JavaUtils.serialize(this)

    override def `type`() = ExternalShuffleMessage.Type.UploadBlock
    override def encodedLength() = serializedVersion.length
    override def encode(buf: ByteBuf) = buf.writeBytes(serializedVersion)
  }

  object UploadBlock {
    def decode(buf: ByteBuf): UploadBlock = JavaUtils.deserialize(buf.array())
  }
}

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 */
class NettyBlockRpcServer(
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  import NettyMessages._

  private val streamManager = new OneForOneStreamManager()

  override def receive(
      client: TransportClient,
      messageBytes: Array[Byte],
      responseContext: RpcResponseCallback): Unit = {
    val ser = serializer.newInstance()

    val buf = Unpooled.wrappedBuffer(messageBytes)
    val typ = ExternalShuffleMessage.Type.decode(buf)
    val message = ser.deserialize[AnyRef](ByteBuffer.wrap(messageBytes))
    logTrace(s"Received request: $message")

    typ match {
      case ExternalShuffleMessage.Type.OpenShuffleBlocks =>
        val openBlocks = OpenShuffleBlocks.decode(buf)
        val blocks: Seq[ManagedBuffer] = openBlocks.blockIds.map(blockManager.getBlockData)
        val streamId = streamManager.registerStream(blocks.iterator)
        logTrace(s"Registered streamId $streamId with ${blocks.size} buffers")
        responseContext.onSuccess(
          ser.serialize(new ShuffleStreamHandle(streamId, blocks.size)).array())

      case ExternalShuffleMessage.Type.UploadBlock =>
        val uploadBlock = UploadBlock.decode(buf)
        blockManager.putBlockData(uploadBlock.blockId,
          new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData)), uploadBlock.level)
        responseContext.onSuccess(new Array[Byte](0))
    }
  }

  override def getStreamManager(): StreamManager = streamManager
}
