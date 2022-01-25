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
import io.dingodb.raft.util.RpcFactoryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class RpcRequestProcessor<T extends Message> implements RpcProcessor<T> {
    protected static final Logger LOG = LoggerFactory.getLogger(RpcRequestProcessor.class);

    private final Executor executor;
    private final Message defaultResp;

    public abstract Message processRequest(final T request, final RpcRequestClosure done);

    public RpcRequestProcessor(Executor executor, Message defaultResp) {
        super();
        this.executor = executor;
        this.defaultResp = defaultResp;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final T request) {
        try {
            final Message msg = processRequest(request, new RpcRequestClosure(rpcCtx, this.defaultResp));
            if (msg != null) {
                rpcCtx.sendResponse(msg);
            }
        } catch (final Throwable t) {
            LOG.error("handleRequest {} failed", request, t);
            rpcCtx.sendResponse(RpcFactoryHelper //
                .responseFactory() //
                .newResponse(defaultResp(), -1, "handleRequest internal error"));
        }
    }

    @Override
    public Executor executor() {
        return this.executor;
    }

    public Message defaultResp() {
        return this.defaultResp;
    }
}
