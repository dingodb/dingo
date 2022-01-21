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

package io.dingodb.store.row.client.pd;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.timer.HashedWheelTimer;
import com.alipay.sofa.jraft.util.timer.Timeout;
import com.alipay.sofa.jraft.util.timer.TimerTask;
import io.dingodb.store.row.StoreEngine;
import io.dingodb.store.row.cmd.pd.BaseRequest;
import io.dingodb.store.row.cmd.pd.BaseResponse;
import io.dingodb.store.row.cmd.pd.StoreHeartbeatRequest;
import io.dingodb.store.row.errors.ErrorsHelper;
import io.dingodb.store.row.metadata.StoreStats;
import io.dingodb.store.row.metadata.TimeInterval;
import io.dingodb.store.row.options.HeartbeatOptions;
import io.dingodb.store.row.rpc.ExtSerializerSupports;
import io.dingodb.store.row.storage.BaseKVStoreClosure;
import io.dingodb.store.row.util.StackTraceUtil;
import io.dingodb.store.row.util.concurrent.DiscardOldPolicyWithReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class StoreHeartbeatSender implements Lifecycle<HeartbeatOptions> {
    private static final Logger LOG = LoggerFactory.getLogger(StoreHeartbeatSender.class);

    private final StoreEngine storeEngine;
    private final PlacementDriverClient pdClient;
    private final RpcClient rpcClient;

    private StatsCollector statsCollector;
    private int heartbeatRpcTimeoutMillis;
    private ThreadPoolExecutor heartbeatRpcCallbackExecutor;
    private HashedWheelTimer heartbeatTimer;

    private boolean started;

    public StoreHeartbeatSender(StoreEngine storeEngine) {
        this.storeEngine = storeEngine;
        this.pdClient = storeEngine.getPlacementDriverClient();
        this.rpcClient = ((AbstractPlacementDriverClient) this.pdClient).getRpcClient();
    }

    @Override
    public synchronized boolean init(final HeartbeatOptions opts) {
        if (this.started) {
            LOG.info("[HeartbeatSender] already started.");
            return true;
        }
        this.statsCollector = new StatsCollector(this.storeEngine);
        this.heartbeatTimer = new HashedWheelTimer(new NamedThreadFactory("heartbeat-timer", true), 50,
            TimeUnit.MILLISECONDS, 4096);
        this.heartbeatRpcTimeoutMillis = opts.getHeartbeatRpcTimeoutMillis();
        if (this.heartbeatRpcTimeoutMillis <= 0) {
            throw new IllegalArgumentException("Heartbeat rpc timeout millis must > 0, "
                                               + this.heartbeatRpcTimeoutMillis);
        }
        final String name = "dingo-row-store-heartbeat-callback";
        this.heartbeatRpcCallbackExecutor = ThreadPoolUtil.newBuilder()
            .poolName(name)
            .enableMetric(true)
            .coreThreads(4)
            .maximumThreads(4)
            .keepAliveSeconds(120L)
            .workQueue(new ArrayBlockingQueue<>(1024))
            .threadFactory(new NamedThreadFactory(name, true))
            .rejectedHandler(new DiscardOldPolicyWithReport(name))
            .build();
        final long storeHeartbeatIntervalSeconds = opts.getStoreHeartbeatIntervalSeconds();
        if (storeHeartbeatIntervalSeconds <= 0) {
            throw new IllegalArgumentException("Store heartbeat interval seconds must > 0, "
                                               + storeHeartbeatIntervalSeconds);
        }
        final long now = System.currentTimeMillis();
        final StoreHeartbeatTask storeHeartbeatTask = new StoreHeartbeatTask(storeHeartbeatIntervalSeconds, now, false);
        this.heartbeatTimer.newTimeout(storeHeartbeatTask, storeHeartbeatTask.getNextDelay(), TimeUnit.SECONDS);
        LOG.info("[StoreHeartbeatSender] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.heartbeatRpcCallbackExecutor);
        if (this.heartbeatTimer != null) {
            this.heartbeatTimer.stop();
        }
    }

    private void sendStoreHeartbeat(final long nextDelay, final boolean forceRefreshLeader, final long lastTime) {
        final long now = System.currentTimeMillis();
        final StoreHeartbeatRequest request = new StoreHeartbeatRequest();
        request.setClusterId(this.storeEngine.getClusterId());
        final TimeInterval timeInterval = new TimeInterval(lastTime, now);
        final StoreStats stats = this.statsCollector.collectStoreStats(timeInterval);
        request.setStats(stats);
        final HeartbeatClosure<Object> closure = new HeartbeatClosure<Object>() {

            @Override
            public void run(final Status status) {
                final boolean forceRefresh = !status.isOk() && ErrorsHelper.isInvalidPeer(getError());
                final StoreHeartbeatTask nexTask = new StoreHeartbeatTask(nextDelay, now, forceRefresh);
                heartbeatTimer.newTimeout(nexTask, nexTask.getNextDelay(), TimeUnit.SECONDS);
            }
        };
        final Endpoint endpoint = this.pdClient.getPdLeader(forceRefreshLeader, this.heartbeatRpcTimeoutMillis);
        callAsyncWithRpc(endpoint, request, closure);
    }

    private <V> void callAsyncWithRpc(final Endpoint endpoint, final BaseRequest request,
                                      final HeartbeatClosure<V> closure) {
        final com.alipay.sofa.jraft.rpc.InvokeContext invokeCtx = new com.alipay.sofa.jraft.rpc.InvokeContext();
        invokeCtx.put(BoltRpcClient.BOLT_CTX, ExtSerializerSupports.getInvokeContext());
        final InvokeCallback invokeCallback = new InvokeCallback() {

            @SuppressWarnings("unchecked")
            @Override
            public void complete(final Object result, final Throwable err) {
                if (err == null) {
                    final BaseResponse<?> response = (BaseResponse<?>) result;
                    if (response.isSuccess()) {
                        closure.setResult((V) response.getValue());
                        closure.run(Status.OK());
                    } else {
                        closure.setError(response.getError());
                        closure.run(new Status(-1, "RPC failed with address: %s, response: %s", endpoint, response));
                    }
                } else {
                    closure.run(new Status(-1, err.getMessage()));
                }
            }

            @Override
            public Executor executor() {
                return heartbeatRpcCallbackExecutor;
            }
        };

        try {
            this.rpcClient.invokeAsync(endpoint, request, invokeCtx, invokeCallback, this.heartbeatRpcTimeoutMillis);
        } catch (final Throwable t) {
            closure.run(new Status(-1, t.getMessage()));
        }
    }

    private abstract static class HeartbeatClosure<V> extends BaseKVStoreClosure {
        private volatile V result;

        public V getResult() {
            return result;
        }

        public void setResult(V result) {
            this.result = result;
        }
    }

    private final class StoreHeartbeatTask implements TimerTask {
        private final long nextDelay;
        private final long lastTime;
        private final boolean forceRefreshLeader;

        private StoreHeartbeatTask(long nextDelay, long lastTime, boolean forceRefreshLeader) {
            this.nextDelay = nextDelay;
            this.lastTime = lastTime;
            this.forceRefreshLeader = forceRefreshLeader;
        }

        @Override
        public void run(final Timeout timeout) throws Exception {
            try {
                sendStoreHeartbeat(this.nextDelay, this.forceRefreshLeader, this.lastTime);
            } catch (final Throwable t) {
                LOG.error("Caught a error on sending [StoreHeartbeat]: {}.", StackTraceUtil.stackTrace(t));
            }
        }

        public long getNextDelay() {
            return nextDelay;
        }
    }

}
