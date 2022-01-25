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

import io.dingodb.raft.option.RpcOptions;
import io.dingodb.raft.util.Endpoint;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface RaftRpcFactory {
    RpcResponseFactory DEFAULT = new RpcResponseFactory() {};

    /**
     * Register serializer with class name.
     *
     * @param className class name
     * @param args      extended parameters, different implementers may need different parameters,
     *                  the order of parameters need a convention
     */
    void registerProtobufSerializer(final String className, final Object... args);

    /**
     * Creates a raft RPC client.
     *
     * @return a new rpc client instance
     */
    default RpcClient createRpcClient() {
        return createRpcClient(null);
    }

    /**
     * Creates a raft RPC client.
     *
     * @param helper config helper for rpc client impl
     * @return a new rpc client instance
     */
    RpcClient createRpcClient(final ConfigHelper<RpcClient> helper);

    /**
     * Creates a raft RPC server.
     *
     * @param endpoint server address to bind
     * @return a new rpc server instance
     */
    default RpcServer createRpcServer(final Endpoint endpoint) {
        return createRpcServer(endpoint, null);
    }

    /**
     * Creates a raft RPC server.
     *
     * @param endpoint server address to bind
     * @param helper   config helper for rpc server impl
     * @return a new rpc server instance
     */
    RpcServer createRpcServer(final Endpoint endpoint, final ConfigHelper<RpcServer> helper);

    default RpcResponseFactory getRpcResponseFactory() {
        return DEFAULT;
    }

    /**
     * Whether to enable replicator pipeline.
     *
     * @return true if enable
     */
    default boolean isReplicatorPipelineEnabled() {
        return true;
    }

    /**
     * Ensure RPC framework supports pipeline.
     */
    default void ensurePipeline() {}

    @SuppressWarnings("unused")
    default ConfigHelper<RpcClient> defaultJRaftClientConfigHelper(final RpcOptions opts) {
        return null;
    }

    @SuppressWarnings("unused")
    default ConfigHelper<RpcServer> defaultJRaftServerConfigHelper(final RpcOptions opts) {
        return null;
    }

    interface ConfigHelper<T> {

        void config(final T instance);
    }
}
