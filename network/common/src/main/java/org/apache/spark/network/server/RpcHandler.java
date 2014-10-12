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

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.SluiceClient;

/**
 * Handler for sendRPC() messages sent by {@link org.apache.spark.network.client.SluiceClient}s.
 */
public interface RpcHandler {
  /**
   * Receive a single RPC message. Any exception thrown while in this method will be sent back to
   * the client in string form as a standard RPC failure.
   */
  void receive(SluiceClient client, byte[] message, RpcResponseCallback callback);
}
