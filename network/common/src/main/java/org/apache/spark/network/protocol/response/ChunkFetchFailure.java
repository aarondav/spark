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

package org.apache.spark.network.protocol.response;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.MessageType;
import org.apache.spark.network.protocol.ResponseMessage;
import org.apache.spark.network.protocol.StreamChunkId;

/**
 * Response to {@link org.apache.spark.network.protocol.request.ChunkFetchRequest} when there is an
 * error fetching the chunk.
 */
public final class ChunkFetchFailure implements ResponseMessage {
  public final StreamChunkId streamChunkId;
  public final String errorString;

  public ChunkFetchFailure(StreamChunkId streamChunkId, String errorString) {
    this.streamChunkId = streamChunkId;
    this.errorString = errorString;
  }

  @Override
  public MessageType type() { return MessageType.ChunkFetchFailure; }

  @Override
  public int encodedLength() {
    return streamChunkId.encodedLength() + 4 + errorString.getBytes().length;
  }

  @Override
  public void encode(ByteBuf buf) {
    streamChunkId.encode(buf);
    byte[] errorBytes = errorString.getBytes();
    buf.writeInt(errorBytes.length);
    buf.writeBytes(errorBytes);
  }

  public static ChunkFetchFailure decode(ByteBuf buf) {
    StreamChunkId streamChunkId = StreamChunkId.decode(buf);
    int numErrorStringBytes = buf.readInt();
    byte[] errorBytes = new byte[numErrorStringBytes];
    buf.readBytes(errorBytes);
    return new ChunkFetchFailure(streamChunkId, new String(errorBytes));
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ChunkFetchFailure) {
      ChunkFetchFailure o = (ChunkFetchFailure) other;
      return streamChunkId.equals(o.streamChunkId) && errorString.equals(o.errorString);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamChunkId", streamChunkId)
      .add("errorString", errorString)
      .toString();
  }
}
