/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.raft.rpc;

import com.google.protobuf.Message;
import io.dingodb.raft.Lifecycle;
import io.dingodb.raft.option.RpcOptions;
import io.dingodb.raft.util.Endpoint;

import java.util.concurrent.Future;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface ClientService extends Lifecycle<RpcOptions> {
    /**
     * Connect to endpoint, returns true when success.
     *
     * @param endpoint server address
     * @return true on connect success
     */
    boolean connect(final Endpoint endpoint);

    /**
     * Check connection for given address and async to create a new one if there is no connection.
     * @param endpoint       target address
     * @param createIfAbsent create a new one if there is no connection
     * @return true if there is a connection and the connection is active and writable.
     */
    boolean checkConnection(final Endpoint endpoint, final boolean createIfAbsent);

    /**
     * Disconnect from endpoint.
     *
     * @param endpoint server address
     * @return true on disconnect success
     */
    boolean disconnect(final Endpoint endpoint);

    /**
     * Returns true when the endpoint's connection is active.
     *
     * @param endpoint server address
     * @return true on connection is active
     */
    boolean isConnected(final Endpoint endpoint);

    /**
     * Send a requests and waits for response with callback, returns the request future.
     *
     * @param endpoint  server address
     * @param request   request data
     * @param done      callback
     * @param timeoutMs timeout millis
     * @return a future with operation result
     */
    <T extends Message> Future<Message> invokeWithDone(final Endpoint endpoint, final Message request,
                                                       final RpcResponseClosure<T> done, final int timeoutMs);
}
