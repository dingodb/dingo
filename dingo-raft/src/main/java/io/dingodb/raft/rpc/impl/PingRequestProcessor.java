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

package io.dingodb.raft.rpc.impl;

import io.dingodb.raft.rpc.RpcContext;
import io.dingodb.raft.rpc.RpcProcessor;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.util.RpcFactoryHelper;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class PingRequestProcessor implements RpcProcessor<RpcRequests.PingRequest> {
    @Override
    public void handleRequest(final RpcContext rpcCtx, final RpcRequests.PingRequest request) {
        rpcCtx.sendResponse( //
                RpcFactoryHelper //
                .responseFactory() //
                .newResponse(RpcRequests.ErrorResponse.getDefaultInstance(), 0, "OK"));
    }

    @Override
    public String interest() {
        return RpcRequests.PingRequest.class.getName();
    }
}
