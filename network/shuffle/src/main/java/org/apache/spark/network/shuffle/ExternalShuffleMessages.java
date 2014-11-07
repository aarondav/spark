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
import java.util.Arrays;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

/** Messages handled by the {@link ExternalShuffleBlockHandler}. */
public class ExternalShuffleMessages {

  public interface ExternalShuffleMessage extends Encodable {
    Type type();

    /** Preceding every serialized Message is its type, which allows us to deserialize it. */
    public static enum Type implements Encodable {
      OpenShuffleBlocks(0), RegisterExecutor(1), UploadBlock(2);

      private final byte id;

      private Type(int id) {
        assert id < 128 : "Cannot have more than 128 message types";
        this.id = (byte) id;
      }

      public byte id() { return id; }

      @Override public int encodedLength() { return 1; }

      @Override public void encode(ByteBuf buf) { buf.writeByte(id); }

      public static Type decode(ByteBuf buf) {
        byte id = buf.readByte();
        switch (id) {
          case 0: return OpenShuffleBlocks;
          case 1: return RegisterExecutor;
          case 2: return UploadBlock;
          default: throw new IllegalArgumentException("Unknown message type: " + id);
        }
      }
    }
  }

  /** Request to read a set of shuffle blocks. Returns [[ShuffleStreamHandle]]. */
  public static class OpenShuffleBlocks implements ExternalShuffleMessage {
    public final String appId;
    public final String execId;
    public final String[] blockIds;

    public OpenShuffleBlocks(String appId, String execId, String[] blockIds) {
      this.appId = appId;
      this.execId = execId;
      this.blockIds = blockIds;
    }

    @Override
    public Type type() { return Type.OpenShuffleBlocks; }

    @Override
    public int hashCode() {
      return Objects.hashCode(appId, execId) * 41 + Arrays.hashCode(blockIds);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("appId", appId)
        .add("execId", execId)
        .add("blockIds", Arrays.toString(blockIds))
        .toString();
    }

    @Override
    public boolean equals(Object other) {
      if (other != null && other instanceof OpenShuffleBlocks) {
        OpenShuffleBlocks o = (OpenShuffleBlocks) other;
        return Objects.equal(appId, o.appId)
          && Objects.equal(execId, o.execId)
          && Arrays.equals(blockIds, o.blockIds);
      }
      return false;
    }

    @Override
    public int encodedLength() {
      return Encoders.Strings.encodedLength(appId)
        + Encoders.Strings.encodedLength(execId)
        + Encoders.StringArrays.encodedLength(blockIds);
    }

    @Override
    public void encode(ByteBuf buf) {
      Encoders.Strings.encode(buf, appId);
      Encoders.Strings.encode(buf, execId);
      Encoders.StringArrays.encode(buf, blockIds);
    }

    public static OpenShuffleBlocks decode(ByteBuf buf) {
      String appId = Encoders.Strings.decode(buf);
      String execId = Encoders.Strings.decode(buf);
      String[] blockIds = Encoders.StringArrays.decode(buf);
      return new OpenShuffleBlocks(appId, execId, blockIds);
    }
  }

  /** Initial registration message between an executor and its local shuffle server. */
  public static class RegisterExecutor implements ExternalShuffleMessage {
    public final String appId;
    public final String execId;
    public final ExecutorShuffleInfo executorInfo;

    public RegisterExecutor(
        String appId,
        String execId,
        ExecutorShuffleInfo executorInfo) {
      this.appId = appId;
      this.execId = execId;
      this.executorInfo = executorInfo;
    }

    @Override
    public Type type() { return Type.RegisterExecutor; }

    @Override
    public int hashCode() {
      return Objects.hashCode(appId, execId, executorInfo);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("appId", appId)
        .add("execId", execId)
        .add("executorInfo", executorInfo)
        .toString();
    }

    @Override
    public boolean equals(Object other) {
      if (other != null && other instanceof RegisterExecutor) {
        RegisterExecutor o = (RegisterExecutor) other;
        return Objects.equal(appId, o.appId)
          && Objects.equal(execId, o.execId)
          && Objects.equal(executorInfo, o.executorInfo);
      }
      return false;
    }

    @Override
    public int encodedLength() {
      return Encoders.Strings.encodedLength(appId)
        + Encoders.Strings.encodedLength(execId)
        + executorInfo.encodedLength();
    }

    @Override
    public void encode(ByteBuf buf) {
      Encoders.Strings.encode(buf, appId);
      Encoders.Strings.encode(buf, execId);
      executorInfo.encode(buf);
    }

    public static RegisterExecutor decode(ByteBuf buf) {
      String appId = Encoders.Strings.decode(buf);
      String execId = Encoders.Strings.decode(buf);
      ExecutorShuffleInfo executorShuffleInfo = ExecutorShuffleInfo.decode(buf);
      return new RegisterExecutor(appId, execId, executorShuffleInfo);
    }
  }
}
