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

package io.dingodb.dingokv;

import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import io.dingodb.dingokv.cmd.store.BatchDeleteRequest;
import io.dingodb.dingokv.cmd.store.BatchPutRequest;
import io.dingodb.dingokv.cmd.store.CASAllRequest;
import io.dingodb.dingokv.cmd.store.CompareAndPutRequest;
import io.dingodb.dingokv.cmd.store.ContainsKeyRequest;
import io.dingodb.dingokv.cmd.store.DeleteRangeRequest;
import io.dingodb.dingokv.cmd.store.DeleteRequest;
import io.dingodb.dingokv.cmd.store.GetAndPutRequest;
import io.dingodb.dingokv.cmd.store.GetRequest;
import io.dingodb.dingokv.cmd.store.GetSequenceRequest;
import io.dingodb.dingokv.cmd.store.KeyLockRequest;
import io.dingodb.dingokv.cmd.store.KeyUnlockRequest;
import io.dingodb.dingokv.cmd.store.MergeRequest;
import io.dingodb.dingokv.cmd.store.MultiGetRequest;
import io.dingodb.dingokv.cmd.store.NodeExecuteRequest;
import io.dingodb.dingokv.cmd.store.PutIfAbsentRequest;
import io.dingodb.dingokv.cmd.store.PutRequest;
import io.dingodb.dingokv.cmd.store.RangeSplitRequest;
import io.dingodb.dingokv.cmd.store.ResetSequenceRequest;
import io.dingodb.dingokv.cmd.store.ScanRequest;
import io.dingodb.dingokv.util.concurrent.CallerRunsPolicyWithReport;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class StoreEngineHelper {
    public static ExecutorService createReadIndexExecutor(final int coreThreads) {
        final int maxThreads = coreThreads << 2;
        final RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();
        return newPool(coreThreads, maxThreads, "dingokv-read-index-callback", handler);
    }

    public static ExecutorService createRaftStateTrigger(final int coreThreads) {
        final BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(32);
        return newPool(coreThreads, coreThreads, "dingokv-raft-state-trigger", workQueue);
    }

    public static ExecutorService createSnapshotExecutor(final int coreThreads, final int maxThreads) {
        return newPool(coreThreads, maxThreads, "dingokv-snapshot-executor");
    }

    public static ExecutorService createCliRpcExecutor(final int coreThreads) {
        final int maxThreads = coreThreads << 2;
        return newPool(coreThreads, maxThreads, "dingokv-cli-rpc-executor");
    }

    public static ExecutorService createRaftRpcExecutor(final int coreThreads) {
        final int maxThreads = coreThreads << 1;
        return newPool(coreThreads, maxThreads, "dingokv-raft-rpc-executor");
    }

    public static ExecutorService createKvRpcExecutor(final int coreThreads) {
        final int maxThreads = coreThreads << 2;
        return newPool(coreThreads, maxThreads, "dingokv-kv-store-rpc-executor");
    }

    public static ScheduledExecutorService createMetricsScheduler() {
        return Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("dingokv-metrics-reporter", true));
    }

    public static void addKvStoreRequestProcessor(final RpcServer rpcServer, final StoreEngine engine) {
        rpcServer.registerProcessor(new KVCommandProcessor<>(GetRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(MultiGetRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(ContainsKeyRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(GetSequenceRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(ResetSequenceRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(ScanRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(PutRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(GetAndPutRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(CompareAndPutRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(MergeRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(PutIfAbsentRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(KeyLockRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(KeyUnlockRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(BatchPutRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(DeleteRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(DeleteRangeRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(BatchDeleteRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(NodeExecuteRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(RangeSplitRequest.class, engine));
        rpcServer.registerProcessor(new KVCommandProcessor<>(CASAllRequest.class, engine));
    }

    private static ExecutorService newPool(final int coreThreads, final int maxThreads, final String name) {
        final RejectedExecutionHandler defaultHandler = new CallerRunsPolicyWithReport(name, name);
        return newPool(coreThreads, maxThreads, name, defaultHandler);
    }

    @SuppressWarnings("SameParameterValue")
    private static ExecutorService newPool(final int coreThreads, final int maxThreads, final String name,
                                           final BlockingQueue<Runnable> workQueue) {
        final RejectedExecutionHandler defaultHandler = new CallerRunsPolicyWithReport(name, name);
        return newPool(coreThreads, maxThreads, workQueue, name, defaultHandler);
    }

    private static ExecutorService newPool(final int coreThreads, final int maxThreads, final String name,
                                           final RejectedExecutionHandler handler) {
        final BlockingQueue<Runnable> defaultWorkQueue = new SynchronousQueue<>();
        return newPool(coreThreads, maxThreads, defaultWorkQueue, name, handler);
    }

    private static ExecutorService newPool(final int coreThreads, final int maxThreads,
                                           final BlockingQueue<Runnable> workQueue, final String name,
                                           final RejectedExecutionHandler handler) {
        return ThreadPoolUtil.newBuilder() //
            .poolName(name) //
            .enableMetric(true) //
            .coreThreads(coreThreads) //
            .maximumThreads(maxThreads) //
            .keepAliveSeconds(60L) //
            .workQueue(workQueue) //
            .threadFactory(new NamedThreadFactory(name, true)) //
            .rejectedHandler(handler) //
            .build();
    }

    private StoreEngineHelper() {
    }
}
