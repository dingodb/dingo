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

package io.dingodb.calcite.utils;

import io.dingodb.common.CommonId;
import io.dingodb.meta.MetaService;
import org.apache.calcite.plan.RelOptTable;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public final class MetaServiceUtils {

    public static final String SCHEMA_NAME = "DINGO";

    private MetaServiceUtils() {
    }

    public static @NonNull CommonId getTableId(RelOptTable table) {
        String tableName = getTableName(table);
        MetaService metaService = getMetaService(table);
        return metaService.getTable(tableName).getTableId();
    }

    public static @NonNull TableInfo getTableInfo(RelOptTable table) {
        String tableName = getTableName(table);
        MetaService metaService = getMetaService(table);
        CommonId tableId = metaService.getTable(tableName).getTableId();
        return new TableInfo(
            tableId,
            metaService.getRangeDistribution(tableId)
        );
    }

    private static String getTableName(@NonNull RelOptTable table) {
        List<String> names = table.getQualifiedName();
        return names.get(names.size() - 1);
    }

    public static MetaService getMetaService(@NonNull RelOptTable table) {
        List<String> names = table.getQualifiedName();
        // ignore 0 root schema
        MetaService metaService = MetaService.root();
        for (int i = 1; i < names.size() - 1; i++) {
            metaService = metaService.getSubMetaService(names.get(i));
        }
        return metaService;
    }

    public static String getSchemaName(String tableName) {
        if (tableName.contains("\\.")) {
            return tableName.split("\\.")[0];
        }
        return SCHEMA_NAME;
    }
}
