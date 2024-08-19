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

import io.dingodb.calcite.schema.SubCalciteSchema;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.meta.entity.Column;

import java.util.List;

public final class SchemaStateUtils {

    private SchemaStateUtils() {
    }

    public static boolean columnHidden(DingoConnection connection, List<String> originList) {
        if (originList == null) {
            return false;
        }
        String schemaName = originList.get(1);
        String tableName = originList.get(2);
        String colName = originList.get(3);
        SubCalciteSchema subCalciteSchema
            = (SubCalciteSchema) connection.getContext().getRootSchema().getSubSchema(schemaName, true);
        if (subCalciteSchema == null) {
            return false;
        }
        io.dingodb.meta.entity.Table table = subCalciteSchema.getTable(tableName);
        Column columnTmp = table.getColumns()
            .stream()
            .filter(column -> column.getName().equalsIgnoreCase(colName))
            .findFirst().orElse(null);
        if (columnTmp == null) {
            return false;
        } else {
            return (columnTmp.getSchemaState() != SchemaState.SCHEMA_PUBLIC
                && columnTmp.getSchemaState() != null) || columnTmp.getState() != 1;
        }
    }
}
