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

package io.dingodb.driver.api;

import io.dingodb.common.annotation.ApiDeclaration;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.Service;

public interface DriverProxyApi {

    @ApiDeclaration
    Service.ResultSetResponse apply(Service.CatalogsRequest request);

    @ApiDeclaration
    Service.ResultSetResponse apply(Service.SchemasRequest request);

    @ApiDeclaration
    Service.ResultSetResponse apply(Service.TablesRequest request);

    @ApiDeclaration
    Service.ResultSetResponse apply(Service.TableTypesRequest request);

    @ApiDeclaration
    Service.ResultSetResponse apply(Service.TypeInfoRequest request);

    @ApiDeclaration
    Service.ResultSetResponse apply(Service.ColumnsRequest request);

    @ApiDeclaration
    Service.PrepareResponse apply(Service.PrepareRequest request);

    @ApiDeclaration
    Service.ExecuteResponse apply(Service.ExecuteRequest request);

    @ApiDeclaration
    Service.ExecuteResponse apply(Service.PrepareAndExecuteRequest request);

    @ApiDeclaration
    Service.SyncResultsResponse apply(Service.SyncResultsRequest request);

    @ApiDeclaration
    Service.FetchResponse apply(Service.FetchRequest request);

    @ApiDeclaration
    Service.CreateStatementResponse apply(Service.CreateStatementRequest request);

    @ApiDeclaration
    Service.CloseStatementResponse apply(Service.CloseStatementRequest request);

    @ApiDeclaration
    Service.OpenConnectionResponse apply(Service.OpenConnectionRequest request);

    @ApiDeclaration
    Service.CloseConnectionResponse apply(Service.CloseConnectionRequest request);

    @ApiDeclaration
    Service.ConnectionSyncResponse apply(Service.ConnectionSyncRequest request);

    @ApiDeclaration
    Service.DatabasePropertyResponse apply(Service.DatabasePropertyRequest request);

    @ApiDeclaration
    Service.CommitResponse apply(Service.CommitRequest request);

    @ApiDeclaration
    Service.RollbackResponse apply(Service.RollbackRequest request);

    @ApiDeclaration
    Service.ExecuteBatchResponse apply(Service.PrepareAndExecuteBatchRequest request);

    @ApiDeclaration
    Service.ExecuteBatchResponse apply(Service.ExecuteBatchRequest request);

    @ApiDeclaration
    void cancel(String connectionId, int id, Meta.Signature signature);
}
