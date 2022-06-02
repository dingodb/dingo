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

package io.dingodb.driver;

import io.dingodb.common.Location;
import io.dingodb.driver.api.DriverProxyApi;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import lombok.Setter;
import lombok.experimental.Delegate;
import org.apache.calcite.avatica.remote.Service;

import java.util.ServiceLoader;
import java.util.function.Supplier;

public class DingoServiceImpl implements Service {

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    @Setter
    private RpcMetadataResponse rpcMetadata;

    @Delegate
    private final DriverProxyApi proxyApi;

    public DingoServiceImpl(Supplier<Location> locationSupplier) {
        proxyApi = netService.apiRegistry().proxy(DriverProxyApi.class, locationSupplier, null, 30);
    }

    public ResultSetResponse apply(CatalogsRequest request) {
        return this.proxyApi.apply(request);
    }

    public ResultSetResponse apply(SchemasRequest request) {
        return this.proxyApi.apply(request);
    }

    public ResultSetResponse apply(TablesRequest request) {
        return this.proxyApi.apply(request);
    }

    public ResultSetResponse apply(TableTypesRequest request) {
        return this.proxyApi.apply(request);
    }

    public ResultSetResponse apply(TypeInfoRequest request) {
        return this.proxyApi.apply(request);
    }

    public ResultSetResponse apply(ColumnsRequest request) {
        return this.proxyApi.apply(request);
    }

    public PrepareResponse apply(PrepareRequest request) {
        return this.proxyApi.apply(request);
    }

    public ExecuteResponse apply(ExecuteRequest request) {
        return this.proxyApi.apply(request);
    }

    public ExecuteResponse apply(PrepareAndExecuteRequest request) {
        return this.proxyApi.apply(request);
    }

    public SyncResultsResponse apply(SyncResultsRequest request) {
        return this.proxyApi.apply(request);
    }

    public FetchResponse apply(FetchRequest request) {
        return this.proxyApi.apply(request);
    }

    public CreateStatementResponse apply(CreateStatementRequest request) {
        return this.proxyApi.apply(request);
    }

    public CloseStatementResponse apply(CloseStatementRequest request) {
        return this.proxyApi.apply(request);
    }

    public OpenConnectionResponse apply(OpenConnectionRequest request) {
        return this.proxyApi.apply(request);
    }

    public CloseConnectionResponse apply(CloseConnectionRequest request) {
        return this.proxyApi.apply(request);
    }

    public ConnectionSyncResponse apply(ConnectionSyncRequest request) {
        return this.proxyApi.apply(request);
    }

    public DatabasePropertyResponse apply(DatabasePropertyRequest request) {
        return this.proxyApi.apply(request);
    }

    public CommitResponse apply(CommitRequest request) {
        return this.proxyApi.apply(request);
    }

    public RollbackResponse apply(RollbackRequest request) {
        return this.proxyApi.apply(request);
    }

    public ExecuteBatchResponse apply(PrepareAndExecuteBatchRequest request) {
        return this.proxyApi.apply(request);
    }

    public ExecuteBatchResponse apply(ExecuteBatchRequest request) {
        return this.proxyApi.apply(request);
    }
}
