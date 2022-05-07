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

import io.dingodb.net.api.annotation.ApiDeclaration;
import org.apache.calcite.avatica.remote.Service;

public interface DriverProxyApi {

    @ApiDeclaration
    String apply(Service.CatalogsRequest request);

    @ApiDeclaration
    String apply(Service.SchemasRequest request);

    @ApiDeclaration
    String apply(Service.TablesRequest request);

    @ApiDeclaration
    String apply(Service.TableTypesRequest request);

    @ApiDeclaration
    String apply(Service.TypeInfoRequest request);

    @ApiDeclaration
    String apply(Service.ColumnsRequest request);

    @ApiDeclaration
    String apply(Service.PrepareRequest request);

    @ApiDeclaration
    String apply(Service.ExecuteRequest request);

    @ApiDeclaration
    String apply(Service.PrepareAndExecuteRequest request);

    @ApiDeclaration
    String apply(Service.SyncResultsRequest request);

    @ApiDeclaration
    String apply(Service.FetchRequest request);

    @ApiDeclaration
    String apply(Service.CreateStatementRequest request);

    @ApiDeclaration
    String apply(Service.CloseStatementRequest request);

    @ApiDeclaration
    String apply(Service.OpenConnectionRequest request);

    @ApiDeclaration
    String apply(Service.CloseConnectionRequest request);

    @ApiDeclaration
    String apply(Service.ConnectionSyncRequest request);

    @ApiDeclaration
    String apply(Service.DatabasePropertyRequest request);

    @ApiDeclaration
    String apply(Service.CommitRequest request);

    @ApiDeclaration
    String apply(Service.RollbackRequest request);

    @ApiDeclaration
    String apply(Service.PrepareAndExecuteBatchRequest request);

    @ApiDeclaration
    String apply(Service.ExecuteBatchRequest request);
}
