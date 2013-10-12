/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.spark.storage

import java.io._
import java.nio.ByteBuffer
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import io.netty.buffer.ByteBuf

/**
 * All forms of block ids should subclass BlockId.
 * If your BlockId should be serializable, be sure to add it to the BlockId.fromString() method.
 */
private[spark] abstract class BlockId {
  def filename: String

  // convenience methods
  def asRDDId = if (isInstanceOf[RDDBlockId]) Some(asInstanceOf[RDDBlockId]) else None
  def isRDD = isInstanceOf[RDDBlockId]
  def isShuffle = isInstanceOf[ShuffleBlockId]
  def isRDDWithId(rddId: Int) =
    this.isInstanceOf[RDDBlockId] && rddId == this.asInstanceOf[RDDBlockId].rddId

  override def toString = filename
  override def hashCode = filename.hashCode
  override def equals(other: Any): Boolean = other match {
    case o: BlockId => filename.equals(o.filename)
    case _ => false
  }
}

trait BlockIdObject {
  val id: Byte
  def fromString(id: String): BlockId
}

case class RDDBlockId(rddId: Int, splitIndex: Int) extends BlockId {
  def filename = "rdd_" + rddId + "_" + splitIndex
}

case class ShuffleBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  def filename = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

case class BroadcastBlockId(broadcastId: Long) extends BlockId {
  def filename = "broadcast_" + broadcastId
}

case class TaskResultBlockId(taskId: Long) extends BlockId {
  def filename = "taskresult_" + taskId
}

case class StreamBlockId(streamId: Int, uniqueId: Long) extends BlockId {
  def filename = "input-" + streamId + "-" + uniqueId
}

// Intended only for testing purposes
case class TestBlockId(id: String) extends BlockId { val filename = "test_" + id }

private[spark] object BlockId {
  val RDD = "rdd_([0-9]+)_([0-9]+)".r
  val Shuffle = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)".r
  val Broadcast = "broadcast_([0-9]+)".r
  val TaskResult = "taskresult_([0-9]+)".r
  val StreamInput = "input-([0-9]+)-([0-9]+)".r
  val Test = "test_(.*)".r

  def fromString(id: String) = id match {
    case RDD(rddId, splitIndex) => RDDBlockId(rddId.toInt, splitIndex.toInt)
    case Shuffle(shuffleId, mapId, reduceId) =>
      ShuffleBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case Broadcast(broadcastId) => BroadcastBlockId(broadcastId.toLong)
    case TaskResult(taskId) => TaskResultBlockId(taskId.toLong)
    case StreamInput(streamId, uniqueId) => StreamBlockId(streamId.toInt, uniqueId.toLong)
    case Test(value) => TestBlockId(value)
    case _ => throw new IllegalStateException("Unknown BlockId type: " + id)
  }

  def writeExternal(out: ObjectOutput, blockId: BlockId) = out.writeUTF(blockId.filename)
  def readExternal(in: ObjectInput): BlockId = fromString(in.readUTF())

  def serialize(blockId: BlockId, byteBuffer: ByteBuffer): Unit = {
    val filename = blockId.filename
    println("Byte butter has space: " + byteBuffer.limit() + " / position: " + byteBuffer.position() +
      " and I want " + filename.getBytes.size + " bytes")
    byteBuffer.putInt(filename.size)
    byteBuffer.put(filename.getBytes)
  }

  def serialize(blockId: BlockId, buf: ByteBuf): Unit = {
    val filename = blockId.filename
    buf.writeInt(filename.length)
    buf.writeBytes(filename.getBytes)
  }

  def deserialize(byteBuffer: ByteBuffer): BlockId = {
    val length = byteBuffer.getInt()
    val bytes = new Array[Byte](length)
    byteBuffer.get(bytes)
    fromString(new String(bytes))
  }

  def deserialize(buf: ByteBuf): BlockId = {
    val length = buf.readInt()
    val bytes = new Array[Byte](length)
    buf.readBytes(bytes)
    fromString(new String(bytes))
  }
}

class ByteBufferBackedInputStream(buf: ByteBuffer) extends InputStream {
  override def read() =
    if (!buf.hasRemaining) -1
    else buf.get() & 0xFF
}
