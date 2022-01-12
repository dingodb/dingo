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

package io.dingodb.store.row;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class RequestProcessClosure<REQ, RSP> implements Closure {
    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessClosure.class);

    private static final AtomicIntegerFieldUpdater<RequestProcessClosure> STATE_UPDATER
        = AtomicIntegerFieldUpdater.newUpdater(RequestProcessClosure.class, "state");

    private static final int PENDING = 0;
    private static final int RESPOND = 1;

    private final REQ request;
    private final RpcContext rpcCtx;

    private RSP response;

    private volatile int state = PENDING;

    public RequestProcessClosure(REQ request, RpcContext rpcCtx) {
        super();
        this.request = request;
        this.rpcCtx = rpcCtx;
    }

    public RpcContext getRpcCtx() {
        return rpcCtx;
    }

    public REQ getRequest() {
        return request;
    }

    public RSP getResponse() {
        return response;
    }

    public void sendResponse(final RSP response) {
        this.response = response;
        run(null);
    }

    /**
     * Run the closure and send response.
     */
    @Override
    public void run(final Status status) {
        if (!STATE_UPDATER.compareAndSet(this, PENDING, RESPOND)) {
            LOG.warn("A response: {} with status: {} sent repeatedly!", this.response, status);
            return;
        }
        this.rpcCtx.sendResponse(this.response);
    }
}
