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

package io.dingodb.calcite;

import io.dingodb.common.CommonId;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Slf4j
public abstract class MutableSchema extends AbstractSchema {
    @Getter
    protected final MetaService metaService;

    protected MutableSchema(MetaService metaService) {
        this.metaService = metaService;
    }

    public CommonId id() {
        return metaService.id();
    }

    public String name() {
        return metaService.name();
    }

    public void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
        metaService.createTable(tableName, tableDefinition);
    }

    public void createIndex(@NonNull String tableName, @NonNull List<Index> indexList) {
        metaService.createIndex(tableName, indexList);
    }

    public void dropIndex(@NonNull String tableName, @NonNull String index) {
        metaService.dropIndex(tableName, index);
    }

    public Collection<Index> getIndex(@NonNull String tableName) {
        TableDefinition tableDefinition = metaService.getTableDefinition(tableName);
        if (tableDefinition != null && tableDefinition.getIndexes() != null) {
            return tableDefinition.getIndexes().values();
        } else {
            return Arrays.asList();
        }
    }

    public boolean dropTable(@NonNull String tableName) {
        return metaService.dropTable(tableName);
    }

    public CommonId getTableId(String tableName) {
        return metaService.getTableId(tableName);
    }

    @Override
    public boolean isMutable() {
        return true;
    }
}
