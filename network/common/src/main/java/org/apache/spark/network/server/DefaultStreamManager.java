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

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * StreamManager which allows registration of an Iterator<ManagedBuffer>, which are individually
 * fetched as chunks by the client.
 */
public class DefaultStreamManager extends StreamManager {
  private final AtomicLong nextStreamId;
  private final Map<Long, StreamState> streams;

  /** State of a single stream. */
  private static class StreamState {
    final Iterator<ManagedBuffer> buffers;

    int curChunk = 0;

    StreamState(Iterator<ManagedBuffer> buffers) {
      this.buffers = buffers;
    }
  }

  public DefaultStreamManager() {
    // Start with a random stream id to help identifying different streams.
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = new ConcurrentHashMap<Long, StreamState>();
  }

  @Override
  public ManagedBuffer getChunk(long streamId, int chunkIndex) {
    StreamState state = streams.get(streamId);
    if (chunkIndex != state.curChunk) {
      throw new IllegalStateException(String.format(
        "Received out-of-order chunk index %s (expected %s)", chunkIndex, state.curChunk));
    } else if (!state.buffers.hasNext()) {
      throw new IllegalStateException(String.format(
        "Requested chunk index beyond end %s", chunkIndex));
    }
    state.curChunk += 1;
    return state.buffers.next();
  }

  @Override
  public void connectionTerminated(long streamId) {
    // Release all remaining buffers.
    StreamState state = streams.remove(streamId);
    if (state != null && state.buffers != null) {
      while (state.buffers.hasNext()) {
        state.buffers.next().release();
      }
    }
  }

  public long registerStream(Iterator<ManagedBuffer> buffers) {
    long myStreamId = nextStreamId.getAndIncrement();
    streams.put(myStreamId, new StreamState(buffers));
    return myStreamId;
  }

  public void unregisterStream(long streamId) {
    streams.remove(streamId);
  }
}
