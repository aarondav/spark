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

import java.nio.{BufferUnderflowException, ByteBuffer}
import java.util

import com.google.common.primitives.Longs
import org.apache.spark.network._
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.server.{DefaultStreamManager, StreamManager, SluiceServer}
import org.apache.spark.network._
import org.apache.spark.network.client.{SluiceClientFactory, SluiceClient}
import org.apache.spark.network.util.{SluiceConfig, ConfigProvider}
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.{ByteBufferInputStream, Utils}
import org.apache.spark.{Logging, SparkConf, SparkEnv}

import scala.collection.JavaConversions._
import scala.concurrent.Future

class NettyBlockTransferService(conf: SparkConf) extends BlockTransferService {
  var client: SluiceClient = _

  val serializer = new JavaSerializer(conf) // TODO!!
  private[this] val sluiceConf = new SluiceConfig(
    new ConfigProvider { override def get(name: String) = conf.get(name) })

  private[this] var server: SluiceServer = _
  private[this] var clientFactory: SluiceClientFactory = _

  override def init(blockDataManager: BlockDataManager): Unit = {
    val streamManager = new DefaultStreamManager
    val rpcHandler = new NettyBlockRpcServer(serializer, streamManager, blockDataManager)
    server = new SluiceServer(sluiceConf, streamManager, rpcHandler)
    clientFactory = new SluiceClientFactory(sluiceConf)
  }

  override def fetchBlocks(
      hostName: String,
      port: Int,
      blockIds: Seq[String],
      listener: BlockFetchingListener): Unit = {
    val client = clientFactory.createClient(hostName, port)
    new NettyBlockFetcher(serializer, client, blockIds, listener)
  }

  override def hostName: String = Utils.localHostName()

  override def port: Int = server.getPort

  override def uploadBlock(hostname: String, port: Int, blockId: String, blockData: ManagedBuffer, level: StorageLevel): Future[Unit] = ???

  override def close(): Unit = server.close()
}
