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
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public abstract class AbstractSchema implements Schema {

    @Getter
    protected final MetaService metaService;
    @Getter
    protected final DingoParserContext context;
    @Getter
    protected final List<String> names;

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

    public synchronized CommonId getTableId(String tableName) {
        return metaService.getTable(tableName.toUpperCase()).getTableId();
    }

    @Override
    public synchronized DingoTable getTable(String name) {
        name = name.toUpperCase();
        Table table = metaService.getTable(name);
        if (table == null) {
            return null;
        }
        return new DingoTable(
            context,
            ImmutableList.<String>builder().addAll(names).add(name).build(),
            metaService.getTableStatistic(table.getTableId()),
            table
        );
    }

    @Override
    public synchronized Set<String> getTableNames() {
        return metaService.getTables().stream().map(Table::getName).collect(Collectors.toSet());
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
    public DingoSchema getSubSchema(String name) {
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
