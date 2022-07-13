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

package io.dingodb.sdk.utils;

import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;

import java.util.ArrayList;
import java.util.List;

public class MetaServiceUtils {

    public static TableDefinition getSimpleTableDefinition(String tableName) {

        List<ColumnDefinition> columns = new ArrayList<>();
        columns.add(ColumnDefinition.getInstance(
            "id",
            "integer",
            null,
            null,
            true,
            true,
            null)
        );
        columns.add(ColumnDefinition.getInstance(
            "name",
            "varchar",
            200,
            0,
            true,
            true,
            null)
        );
        columns.add(ColumnDefinition.getInstance(
            "salary",
            "double",
            null,
            3,
            true,
            true,
            null)
        );
        TableDefinition tableDefinition = new TableDefinition(tableName);
        tableDefinition.setColumns(columns);
        return tableDefinition;
    }
}
