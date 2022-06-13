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

import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface RpcProcessor<T> {
    /**
     * Async to handle request with {@link RpcContext}.
     *
     * @param rpcCtx  the rpc context
     * @param request the request
     */
    void handleRequest(final RpcContext rpcCtx, final T request);

    /**
     * The class name of user request.
     * Use String type to avoid loading class.
     *
     * @return interested request's class name
     */
    String interest();

    /**
     * Get user's executor.
     *
     * @return executor
     */
    default Executor executor() {
        return null;
    }

    /**
     *
     * @return the executor selector
     */
    default ExecutorSelector executorSelector() {
        return null;
    }

    /**
     * Executor selector interface.
     */
    interface ExecutorSelector {

        /**
         * Select a executor.
         *
         * @param reqClass  request class name
         * @param reqHeader request header
         * @return a executor
         */
        Executor select(final String reqClass, final Object reqHeader);
    }
}
