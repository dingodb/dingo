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

package io.dingodb.server.executor.ddl;

import io.dingodb.sdk.service.entity.common.SchemaState;
import io.dingodb.sdk.service.entity.meta.ColumnDefinition;
import io.dingodb.sdk.service.entity.meta.TableDefinitionWithId;

import java.util.List;

public final class DdlColumn {
    private DdlColumn() {
    }

    public static void generateOriginDefaultValue(ColumnDefinition columnDef) {
        Object odVal = columnDef.getDefaultVal();
        if (odVal == null && !columnDef.isNullable()) {
            switch (columnDef.getSqlType()) {
                case "ENUM":
                    break;
                default:
                    columnDef.setDefaultVal("0");
            }
        }
    }

    public static void setIndicesState(List<TableDefinitionWithId> indexInfoList, SchemaState schemaState) {
        if (indexInfoList == null) {
            return;
        }
        indexInfoList.forEach(indexInfo -> {
            indexInfo.getTableDefinition().setSchemaState(schemaState);
            TableUtil.updateIndex(indexInfo);
        });
    }

}
