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

package io.dingodb.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoTable;
import io.dingodb.common.CommonId;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public abstract class AbstractSchema implements Schema {

    @Getter
    protected final MetaService metaService;
    @Getter
    protected final DingoParserContext context;
    @Getter
    protected final List<String> names;

    protected final Map<String, DingoTable> tableCache = new ConcurrentHashMap<>();

    AbstractSchema(MetaService metaService, DingoParserContext context, List<String> names) {
        this.metaService = metaService;
        this.context = context;
        this.names = names;
    }

    public CommonId id() {
        return metaService.id();
    }

    public String name() {
        return metaService.name();
    }

    protected void addTableCache(String name, TableDefinition td) {
        if (tableCache.containsKey(name)) {
            return;
        }
        CommonId tableId = metaService.getTableId(name);
        DingoTable table = new DingoTable(tableId,
            context,
            ImmutableList.<String>builder().addAll(names).add(name).build(),
            td,
            metaService.getTableStatistic(name)
        );
        tableCache.put(name, table);
    }

    public synchronized CommonId getTableId(String tableName) {
        return tableCache.get(tableName).getTableId();
    }

    @Override
    public synchronized Table getTable(String name) {
        name = name.toUpperCase();
        if (tableCache.get(name) == null) {
            // todo change coordinator get table return null, throw exception currently
            if (metaService.getTableId(name) == null) {
                return null;
            }
            TableDefinition tableDefinition = metaService.getTableDefinition(name);

            if (tableDefinition != null) {
                addTableCache(name, tableDefinition);
            }
        }
        return tableCache.get(name);
    }

    @Override
    public synchronized Set<String> getTableNames() {
        if (tableCache.isEmpty()) {
            metaService.getTableDefinitions().forEach(this::addTableCache);
        }
        return metaService.getTableDefinitions().keySet();
    }

    @Override
    public RelProtoDataType getType(String name) {
        return null;
    }

    @Override
    public Set<String> getTypeNames() {
        return ImmutableSet.of();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return ImmutableSet.of();
    }

    @Override
    public Set<String> getFunctionNames() {
        return ImmutableSet.of();
    }

    @Override
    public Schema getSubSchema(String name) {
        return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return ImmutableSet.of();
    }

    @Override
    public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
        requireNonNull(parentSchema, "parentSchema");
        return Schemas.subSchemaExpression(parentSchema, name, getClass());
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }

}
