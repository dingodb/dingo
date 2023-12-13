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

package io.dingodb.store.proxy.mapper;

import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.DropSchemaRequest;
import io.dingodb.sdk.service.entity.meta.GetSchemasRequest;
import io.dingodb.sdk.service.entity.meta.GetTableByNameRequest;
import io.dingodb.sdk.service.entity.meta.GetTablesBySchemaRequest;

public interface RequestResponseMapper {
    GetTablesBySchemaRequest getTablesBySchema(DingoCommonId schemaId);

    GetTableByNameRequest getTableByName(DingoCommonId schemaId, String tableName);

    GetSchemasRequest getSchemas(DingoCommonId schemaId);

    DropSchemaRequest dropSchema(DingoCommonId schemaId);

}
