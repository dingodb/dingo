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

package io.dingodb.dingokv.client;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import io.dingodb.dingokv.client.failover.FailoverClosure;
import io.dingodb.dingokv.client.pd.AbstractPlacementDriverClient;
import io.dingodb.dingokv.client.pd.PlacementDriverClient;
import io.dingodb.dingokv.cmd.store.BaseRequest;
import io.dingodb.dingokv.cmd.store.BaseResponse;
import io.dingodb.dingokv.errors.Errors;
import io.dingodb.dingokv.errors.ErrorsHelper;
import io.dingodb.dingokv.options.RpcOptions;
import io.dingodb.dingokv.rpc.ExtSerializerSupports;
import io.dingodb.dingokv.util.concurrent.CallerRunsPolicyWithReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class DefaultDingoKVRpcService implements DingoKVRpcService {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDingoKVRpcService.class);

    private final PlacementDriverClient pdClient;
    private final RpcClient rpcClient;
    private final Endpoint selfEndpoint;

    private ThreadPoolExecutor rpcCallbackExecutor;
    private int rpcTimeoutMillis;

    private boolean started;

    public DefaultDingoKVRpcService(PlacementDriverClient pdClient, Endpoint selfEndpoint) {
        this.pdClient = pdClient;
        this.rpcClient = ((AbstractPlacementDriverClient) pdClient).getRpcClient();
        this.selfEndpoint = selfEndpoint;
    }

    @Override
    public synchronized boolean init(final RpcOptions opts) {
        if (this.started) {
            LOG.info("[DefaultDingoKVRpcService] already started.");
            return true;
        }
        this.rpcCallbackExecutor = createRpcCallbackExecutor(opts);
        this.rpcTimeoutMillis = opts.getRpcTimeoutMillis();
        Requires.requireTrue(this.rpcTimeoutMillis > 0, "opts.rpcTimeoutMillis must > 0");
        LOG.info("[DefaultDingoKVRpcService] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.rpcCallbackExecutor);
        this.started = false;
        LOG.info("[DefaultDingoKVRpcService] shutdown successfully.");
    }

    @Override
    public <V> CompletableFuture<V> callAsyncWithRpc(final BaseRequest request, final FailoverClosure<V> closure,
                                                     final Errors lastCause) {
        return callAsyncWithRpc(request, closure, lastCause, true);
    }

    @Override
    public <V> CompletableFuture<V> callAsyncWithRpc(final BaseRequest request, final FailoverClosure<V> closure,
                                                     final Errors lastCause, final boolean requireLeader) {
        final boolean forceRefresh = ErrorsHelper.isInvalidPeer(lastCause);
        final Endpoint endpoint = getRpcEndpoint(request.getRegionId(), forceRefresh, this.rpcTimeoutMillis,
            requireLeader);
        internalCallAsyncWithRpc(endpoint, request, closure);
        return closure.future();
    }

    public Endpoint getLeader(final String regionId, final boolean forceRefresh, final long timeoutMillis) {
        return this.pdClient.getLeader(regionId, forceRefresh, timeoutMillis);
    }

    public Endpoint getLuckyPeer(final String regionId, final boolean forceRefresh, final long timeoutMillis) {
        return this.pdClient.getLuckyPeer(regionId, forceRefresh, timeoutMillis, this.selfEndpoint);
    }

    public Endpoint getRpcEndpoint(final String regionId, final boolean forceRefresh, final long timeoutMillis,
                                   final boolean requireLeader) {
        if (requireLeader) {
            return getLeader(regionId, forceRefresh, timeoutMillis);
        } else {
            return getLuckyPeer(regionId, forceRefresh, timeoutMillis);
        }
    }

    private <V> void internalCallAsyncWithRpc(final Endpoint endpoint, final BaseRequest request,
                                              final FailoverClosure<V> closure) {
        final InvokeContext invokeCtx = new InvokeContext();
        invokeCtx.put(BoltRpcClient.BOLT_CTX, ExtSerializerSupports.getInvokeContext());
        final InvokeCallback invokeCallback = new InvokeCallback() {

            @Override
            public void complete(final Object result, final Throwable err) {
                if (err == null) {
                    final BaseResponse<?> response = (BaseResponse<?>) result;
                    if (response.isSuccess()) {
                        closure.setData(response.getValue());
                        closure.run(Status.OK());
                    } else {
                        closure.setError(response.getError());
                        closure.run(new Status(-1, "RPC failed with address: %s, response: %s", endpoint, response));
                    }
                } else {
                    closure.failure(err);
                }
            }

            @Override
            public Executor executor() {
                return rpcCallbackExecutor;
            }
        };

        try {
            this.rpcClient.invokeAsync(endpoint, request, invokeCtx, invokeCallback, this.rpcTimeoutMillis);
        } catch (final Throwable t) {
            closure.failure(t);
        }
    }

    private ThreadPoolExecutor createRpcCallbackExecutor(final RpcOptions opts) {
        final int callbackExecutorCorePoolSize = opts.getCallbackExecutorCorePoolSize();
        final int callbackExecutorMaximumPoolSize = opts.getCallbackExecutorMaximumPoolSize();
        if (callbackExecutorCorePoolSize <= 0 || callbackExecutorMaximumPoolSize <= 0) {
            return null;
        }

        final String name = "dingokv-rpc-callback";
        return ThreadPoolUtil.newBuilder() //
            .poolName(name) //
            .enableMetric(true) //
            .coreThreads(callbackExecutorCorePoolSize) //
            .maximumThreads(callbackExecutorMaximumPoolSize) //
            .keepAliveSeconds(120L) //
            .workQueue(new ArrayBlockingQueue<>(opts.getCallbackExecutorQueueCapacity())) //
            .threadFactory(new NamedThreadFactory(name, true)) //
            .rejectedHandler(new CallerRunsPolicyWithReport(name)) //
            .build();
    }
}
