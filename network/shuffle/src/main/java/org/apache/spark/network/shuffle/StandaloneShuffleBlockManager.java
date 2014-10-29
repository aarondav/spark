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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.JavaUtils;

/**
 * Manages converting shuffle BlockIds into physical segments of local files. Each Executor must
 * register its own configuration about where it stores its files (local dirs) and how (shuffle
 * manager). The logic for retrieval of individual files is replicated from Spark's
 * FileShuffleBlockManager and IndexShuffleBlockManager.
 *
 * Executors with shuffle file consolidation are not currently supported, as the index is stored in
 * the Executor's memory, unlike the IndexShuffleBlockManager.
 */
public class StandaloneShuffleBlockManager {
  private final Logger logger = LoggerFactory.getLogger(StandaloneShuffleBlockManager.class);

  // Map from "appId-execId" to the executor's configuration.
  private final ConcurrentHashMap<String, ExecutorConfig> executors =
    new ConcurrentHashMap<String, ExecutorConfig>();

  /** Contains all configuration necessary for a single Executor to find its shuffle files. */
  private static class ExecutorConfig {
    /** The base set of local directories that the executor stores its shuffle files in. */
    final String[] localDirs;
    /** Number of subdirectories created within each localDir. */
    final int subDirsPerLocalDir;
    /** Shuffle manager (SortShuffleManager or HashShuffleManager) that the executor is using. */
    final String shuffleManager;

    private ExecutorConfig(String[] localDirs, int subDirsPerLocalDir, String shuffleManager) {
      this.localDirs = localDirs;
      this.subDirsPerLocalDir = subDirsPerLocalDir;
      this.shuffleManager = shuffleManager;
    }
  }

  // Returns an id suitable for a single executor within a single application.
  private String getAppExecId(String appId, String execId) {
    return appId + "-" + execId;
  }

  /** Registers a new Executor with all the configuration we need to find its shuffle files. */
  public void registerExecutor(
      String appId,
      String execId,
      String[] localDirs,
      int subDirsPerLocalDir,
      String shuffleManager) {
    String fullId = getAppExecId(appId, execId);
    executors.put(fullId, new ExecutorConfig(localDirs, subDirsPerLocalDir, shuffleManager));
  }

  /**
   * Obtains a FileSegmentManagedBuffer from a shuffle block id. We expect the blockId has the
   * format "shuffle_ShuffleId_MapId_ReduceId", and additionally make assumptions about how the
   * hash and sort based shuffles store their data.
   */
  public ManagedBuffer getBlockData(String appId, String execId, String blockId) {
    String[] blockIdParts = blockId.split("_");
    if (blockIdParts.length < 4) {
      throw new IllegalArgumentException("Unexpected block id format: " + blockId);
    } else if (!blockIdParts[0].equals("shuffle")) {
      throw new IllegalArgumentException("Expected shuffle block id, got: " + blockId);
    }
    int shuffleId = Integer.parseInt(blockIdParts[1]);
    int mapId = Integer.parseInt(blockIdParts[2]);
    int reduceId = Integer.parseInt(blockIdParts[3]);

    ExecutorConfig executor = executors.get(getAppExecId(appId, execId));
    if (executor == null) {
      throw new RuntimeException(
        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId));
    }

    if ("org.apache.spark.shuffle.hash.HashShuffleManager".equals(executor.shuffleManager)) {
      return getHashBasedShuffleBlockData(executor, blockId);
    } else if ("org.apache.spark.shuffle.sort.SortShuffleManager".equals(executor.shuffleManager)) {
      return getSortBasedShuffleBlockData(executor, shuffleId, mapId, reduceId);
    } else {
      throw new UnsupportedOperationException(
        "Unsupported shuffle manager: " + executor.shuffleManager);
    }
  }

  /**
   * Hash-based shuffle data is simply stored as one file per block.
   * This logic is from FileShuffleBlockManager.
   */
  // TODO: Support consolidated hash shuffle files
  private ManagedBuffer getHashBasedShuffleBlockData(ExecutorConfig executor, String blockId) {
    File shuffleFile = getFile(executor.localDirs, executor.subDirsPerLocalDir, blockId);
    return new FileSegmentManagedBuffer(shuffleFile, 0, shuffleFile.length());
  }

  /**
   * Sort-based shuffle data uses an index called "shuffle_ShuffleId_MapId_0.index" into a data file
   * called "shuffle_ShuffleId_MapId_0.data". This logic is from IndexShuffleBlockManager.
   */
  private ManagedBuffer getSortBasedShuffleBlockData(
    ExecutorConfig executor, int shuffleId, int mapId, int reduceId) {
    File indexFile = getFile(executor.localDirs, executor.subDirsPerLocalDir,
      "shuffle_" + shuffleId + "_" + mapId + "_0.index");

    DataInputStream in = null;
    try {
      in = new DataInputStream(new FileInputStream(indexFile));
      in.skipBytes(reduceId * 8);
      long offset = in.readLong();
      long nextOffset = in.readLong();
      return new FileSegmentManagedBuffer(
        getFile(executor.localDirs, executor.subDirsPerLocalDir,
          "shuffle_" + shuffleId + "_" + mapId + "_0.data"),
        offset,
        nextOffset - offset);
    } catch (IOException e) {
      throw new RuntimeException("Failed to open file: " + indexFile, e);
    } finally {
      if (in != null) {
        JavaUtils.closeQuietly(in);
      }
    }
  }

  /**
   * Hashes a filename into the corresponding local directory, in a manner consistent with
   * Spark's DiskBlockManager.getFile().
   */
  private File getFile(String[] localDirs, int subDirsPerLocalDir, String filename) {
    int hash = nonNegativeHash(filename);
    String localDir = localDirs[hash % localDirs.length];
    int subDirId = (hash / localDirs.length) % subDirsPerLocalDir;
    return new File(new File(localDir, String.format("%02x", subDirId)), filename);
  }

  /** Returns a hash consistent with Spark's Utils.nonNegativeHash(). */
  private int nonNegativeHash(Object obj) {
    if (obj == null) { return 0; }
    int hash = obj.hashCode();
    return hash != Integer.MIN_VALUE ? Math.abs(hash) : 0;
  }
}
