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

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.SluiceClient;
import org.apache.spark.network.client.SluiceResponseHandler;

public class MessageDispatcherFactory {
  private final Logger logger = LoggerFactory.getLogger(MessageDispatcherFactory.class);

  private final StreamManager streamManager;
  private final RpcHandler rpcHandler;

  public MessageDispatcherFactory(StreamManager streamManager, RpcHandler rpcHandler) {
    this.streamManager = streamManager;
    this.rpcHandler = rpcHandler;
  }

  public MessageDispatcher createDispatcher(Channel channel) {
    try {
      SluiceResponseHandler responseHandler = new SluiceResponseHandler(channel);
      SluiceClient client = new SluiceClient(channel, responseHandler);
      SluiceRequestHandler requestHandler = new SluiceRequestHandler(channel, client, streamManager,
        rpcHandler);
      return new MessageDispatcher(client, responseHandler, requestHandler);
    } catch (RuntimeException e) {
      logger.error("Error while creating MessageDispatcher", e);
      throw e;
    }
  }
}
