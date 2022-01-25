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

import io.dingodb.raft.util.Endpoint;
import io.dingodb.store.row.client.FutureHelper;
import io.dingodb.store.row.client.failover.FailoverClosure;
import io.dingodb.store.row.client.failover.RetryRunner;
import io.dingodb.store.row.client.failover.impl.FailoverClosureImpl;
import io.dingodb.store.row.cmd.pd.CreateRegionIdRequest;
import io.dingodb.store.row.cmd.pd.GetClusterInfoRequest;
import io.dingodb.store.row.cmd.pd.GetStoreIdRequest;
import io.dingodb.store.row.cmd.pd.GetStoreInfoRequest;
import io.dingodb.store.row.cmd.pd.SetStoreInfoRequest;
import io.dingodb.store.row.errors.Errors;
import io.dingodb.store.row.metadata.Cluster;
import io.dingodb.store.row.metadata.Store;

import java.util.concurrent.CompletableFuture;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class MetadataRpcClient {
    private final PlacementDriverRpcService pdRpcService;
    private final int failoverRetries;

    public MetadataRpcClient(PlacementDriverRpcService pdRpcService, int failoverRetries) {
        this.pdRpcService = pdRpcService;
        this.failoverRetries = failoverRetries;
    }

    /**
     * Returns the specified cluster information.
     */
    public Cluster getClusterInfo(final long clusterId) {
        final CompletableFuture<Cluster> future = new CompletableFuture<>();
        internalGetClusterInfo(clusterId, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalGetClusterInfo(final long clusterId, final CompletableFuture<Cluster> future,
                                        final int retriesLeft, final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalGetClusterInfo(clusterId, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Cluster> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final GetClusterInfoRequest request = new GetClusterInfoRequest();
        request.setClusterId(clusterId);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * The pd server stores the storeIds of all nodes.
     * This method provides a lookup for the storeId according
     * to the host.  If there is no value, then a globally
     * unique storeId is created.
     */
    public Long getOrCreateStoreId(final long clusterId, final Endpoint endpoint) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        internalGetOrCreateStoreId(clusterId, endpoint, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalGetOrCreateStoreId(final long clusterId, final Endpoint endpoint,
                                            final CompletableFuture<Long> future, final int retriesLeft,
                                            final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalGetOrCreateStoreId(clusterId, endpoint, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Long> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final GetStoreIdRequest request = new GetStoreIdRequest();
        request.setClusterId(clusterId);
        request.setEndpoint(endpoint);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * Query the store information by the host.  If the result
     * is a empty instance, the caller needs to use its own local
     * configuration.
     */
    public Store getStoreInfo(final long clusterId, final Endpoint selfEndpoint) {
        final CompletableFuture<Store> future = new CompletableFuture<>();
        internalGetStoreInfo(clusterId, selfEndpoint, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalGetStoreInfo(final long clusterId, final Endpoint selfEndpoint,
                                      final CompletableFuture<Store> future, final int retriesLeft,
                                      final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalGetStoreInfo(clusterId, selfEndpoint, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Store> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final GetStoreInfoRequest request = new GetStoreInfoRequest();
        request.setClusterId(clusterId);
        request.setEndpoint(selfEndpoint);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * Update the store information by the storeId.
     */
    public Store updateStoreInfo(final long clusterId, final Store store) {
        final CompletableFuture<Store> future = new CompletableFuture<>();
        internalUpdateStoreInfo(clusterId, store, future, 1, null);
        return FutureHelper.get(future);
    }

    private void internalUpdateStoreInfo(final long clusterId, final Store store, final CompletableFuture<Store> future,
                                         final int retriesLeft, final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalUpdateStoreInfo(clusterId, store, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Store> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final SetStoreInfoRequest request = new SetStoreInfoRequest();
        request.setClusterId(clusterId);
        request.setStore(store);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * Create a globally unique regionId.
     */
    public Long createRegionId(final long clusterId, final Endpoint endpoint) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        internalCreateRegionId(clusterId, endpoint, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalCreateRegionId(final long clusterId, final Endpoint endpoint,
                                        final CompletableFuture<Long> future, final int retriesLeft,
                                        final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalCreateRegionId(clusterId, endpoint, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Long> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final CreateRegionIdRequest request = new CreateRegionIdRequest();
        request.setClusterId(clusterId);
        request.setEndpoint(endpoint);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }
}
