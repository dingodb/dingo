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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.dingodb.net.Channel;
import io.dingodb.raft.Lifecycle;
import io.dingodb.raft.Status;
import io.dingodb.raft.error.InvokeTimeoutException;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.option.RpcOptions;
import io.dingodb.raft.rpc.impl.FutureImpl;
import io.dingodb.raft.util.Endpoint;

import java.util.concurrent.Future;
import java.util.function.Function;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface ClientService extends Lifecycle<RpcOptions> {

    /**
     * Check connection for given address and async to create a new one if there is no connection.
     * @param endpoint       target address
     * @param createIfAbsent create a new one if there is no connection
     * @return true if there is a connection and the connection is active and writable.
     */
    boolean checkConnection(final Endpoint endpoint, final boolean createIfAbsent);

    public <T extends Message> Future<Message> invokeWithDone(Message result, RpcResponseClosure<T> done,
                                                              FutureImpl<Message> future, Channel channel);
}
