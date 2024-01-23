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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import io.dingodb.common.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.NameMap;
import org.apache.calcite.util.NameMultimap;
import org.apache.calcite.util.NameSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

@Slf4j
public class DingoCalciteSchema extends CalciteSchema {

    public final AbstractSchema schema;

    @Builder
    public DingoCalciteSchema(
        CalciteSchema parent,
        AbstractSchema schema,
        String name,
        NameMap<CalciteSchema> subSchemas,
        NameMap<TableEntry> tables,
        NameMap<LatticeEntry> lattices,
        NameMap<TypeEntry> types,
        NameMultimap<FunctionEntry> functions,
        NameSet functionNames,
        NameMap<FunctionEntry> nullaryFunctions,
        List<? extends List<String>> path
    ) {
        super(
            parent, schema, name, subSchemas, tables, lattices, types, functions, functionNames, nullaryFunctions, path
        );
        this.schema = schema;
    }

    @Override
    protected @Nullable CalciteSchema getImplicitSubSchema(String schemaName, boolean caseSensitive) {
        String name = schemaName.toUpperCase();
        return Optional.mapOrNull(
            schema.getSubSchema(name),
            $ -> builder().schema($).name(name).build()
        );
    }

    @Override
    protected TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
        String name = tableName.toUpperCase();
        return Optional.mapOrNull(
            schema.getTable(name),
            $ -> new TableEntryImpl(this, name, $, ImmutableList.of())
        );
    }

    @Override
    protected TypeEntry getImplicitType(String name, boolean caseSensitive) {
        return null;
    }

    @Override
    protected TableEntry getImplicitTableBasedOnNullaryFunction(String tableName, boolean caseSensitive) {
        return null;
    }

    @Override
    protected void addImplicitSubSchemaToBuilder(ImmutableSortedMap.Builder<String, CalciteSchema> builder) {
        if (schema instanceof DingoRootSchema) {
            DingoRootSchema rootSchema = (DingoRootSchema) schema;
            rootSchema.getSubSchemas().forEach((k, v) -> builder.put(k, builder().schema(v).name(k).build()));
        }
    }

    @Override
    protected void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {
        schema.getTableNames().forEach(builder::add);
    }

    @Override
    protected void addImplicitFunctionsToBuilder(ImmutableList.Builder<Function> builder, String name, boolean caseSensitive) {
        // ignore
    }

    @Override
    protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
        // ignore
    }

    @Override
    protected void addImplicitTypeNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
        // ignore
    }

    @Override
    protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(ImmutableSortedMap.Builder<String, Table> builder) {
        // ignore
    }

    @Override
    protected CalciteSchema snapshot(@Nullable CalciteSchema parent, SchemaVersion version) {
        return null;
    }

    @Override
    protected boolean isCacheEnabled() {
        return true;
    }

    @Override
    public void setCache(boolean cache) {
        // ignore
    }

    @Override
    public CalciteSchema add(String name, Schema schema) {
        return null;
    }

}
