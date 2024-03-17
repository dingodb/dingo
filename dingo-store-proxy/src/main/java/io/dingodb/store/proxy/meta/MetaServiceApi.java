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

package io.dingodb.store.proxy.meta;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.sdk.service.entity.meta.CreateSchemaRequest;
import io.dingodb.sdk.service.entity.meta.CreateTablesRequest;
import io.dingodb.sdk.service.entity.meta.DropSchemaRequest;
import io.dingodb.sdk.service.entity.meta.DropTablesRequest;
import io.dingodb.transaction.api.TableLock;

import java.util.List;
import java.util.Map;

public interface MetaServiceApi {

    @ApiDeclaration
    void connect(Channel channel, CommonId serverId, Location location) throws Exception;

    @ApiDeclaration
    void lockTable(long requestId, TableLock lock) throws Exception;

    @ApiDeclaration
    Location getLocation(CommonId serverId) throws Exception;

    @ApiDeclaration
    void syncTableLock(TableLock lock) throws Exception;

    @ApiDeclaration
    void createTables(long requestId, String schema, String table, CreateTablesRequest request) throws Exception;

    @ApiDeclaration
    void dropTables(long requestId, String schema, String table, DropTablesRequest request) throws Exception;

    @ApiDeclaration
    void createSchema(long requestId, String schema, CreateSchemaRequest request) throws Exception;

    @ApiDeclaration
    void dropSchema(long requestId, String schema, DropSchemaRequest request) throws Exception;

}
