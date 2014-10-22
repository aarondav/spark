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

import java.io.Serializable;

public class ShuffleMessages {

  /** Request to read a set of blocks. Returns [[ShuffleStreamHandle]] to identify the stream. */
  public static class OpenShuffleBlocks implements Serializable {
    public final String appId;
    public final String execId;
    public final String[] blockIds;

    public OpenShuffleBlocks(String appId, String execId, String[] blockIds) {
      this.appId = appId;
      this.execId = execId;
      this.blockIds = blockIds;
    }
  }

  /** Identifier for a fixed number of chunks to read from a stream created by [[OpenBlocks]]. */
  public static class ShuffleStreamHandle implements Serializable {
    public final long streamId;
    public final int numChunks;

    public ShuffleStreamHandle(long streamId, int numChunks) {
      this.streamId = streamId;
      this.numChunks = numChunks;
    }
  }

  public static class RegisterExecutor implements Serializable {
    public final String appId;
    public final String execId;
    public final String[] localDirs;
    public final int subDirsPerLocalDir;
    public final String shuffleManager;

    public RegisterExecutor(
        String appId,
        String execId,
        String[] localDirs,
        int subDirsPerLocalDir,
        String shuffleManager) {
      this.appId = appId;
      this.execId = execId;
      this.localDirs = localDirs;
      this.subDirsPerLocalDir = subDirsPerLocalDir;
      this.shuffleManager = shuffleManager;
    }
  }
}
