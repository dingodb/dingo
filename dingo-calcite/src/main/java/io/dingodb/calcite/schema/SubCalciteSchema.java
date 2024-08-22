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
import io.dingodb.calcite.DingoTable;
import io.dingodb.common.log.LogUtils;
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
public class SubCalciteSchema extends CalciteSchema {
    RootCalciteSchema rootCalciteSchema;

    @Builder
    protected SubCalciteSchema(
        CalciteSchema parent,
        Schema schema,
        String name,
        NameMap<CalciteSchema> subSchemas,
        NameMap<TableEntry> tables,
        NameMap<LatticeEntry> lattices,
        NameMap<TypeEntry> types,
        NameMultimap<FunctionEntry> functions,
        NameSet functionNames,
        NameMap<FunctionEntry> nullaryFunctions,
        List<? extends List<String>> path,
        RootCalciteSchema rootCalciteSchema
    ) {
        super(
            parent, schema, name, subSchemas, tables, lattices, types, functions, functionNames, nullaryFunctions, path
        );
        this.rootCalciteSchema = rootCalciteSchema;
    }

    @Override
    protected @Nullable CalciteSchema getImplicitSubSchema(String schemaName, boolean caseSensitive) {
        return null;
    }

    @Override
    public @Nullable TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        boolean forValidate = false;
        if (stackTrace.length > 19) {
            forValidate = stackTrace[19].getMethodName().equalsIgnoreCase("validate");
        }
        String name = tableName.toUpperCase();
        SubSnapshotSchema subSnapshotSchema = (SubSnapshotSchema) schema;
        boolean inTxn = subSnapshotSchema.inTransaction();
        Table table;
        if (forValidate) {
            table = ((SubSnapshotSchema) schema).getValidateTable(name);
        } else {
            table = schema.getTable(name);
        }
        if (table != null && inTxn) {
            DingoTable dingoTable = (DingoTable) table;
            rootCalciteSchema.putRelatedTable(dingoTable.getTableId().seq, ((SubSnapshotSchema) schema).getSchemaVer());
        }
        return Optional.mapOrNull(
            table,
            $ -> new TableEntryImpl(this, name, $, ImmutableList.of())
        );
    }

    @Override
    protected @Nullable TypeEntry getImplicitType(String name, boolean caseSensitive) {
        return null;
    }

    @Override
    protected @Nullable TableEntry getImplicitTableBasedOnNullaryFunction(String tableName, boolean caseSensitive) {
        return null;
    }

    @Override
    protected void addImplicitSubSchemaToBuilder(ImmutableSortedMap.Builder<String, CalciteSchema> builder) {

    }

    @Override
    protected void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {
        schema.getTableNames().forEach(builder::add);
    }

    @Override
    protected void addImplicitFunctionsToBuilder(ImmutableList.Builder<Function> builder, String name, boolean caseSensitive) {

    }

    @Override
    protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {

    }

    @Override
    protected void addImplicitTypeNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {

    }

    @Override
    protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(ImmutableSortedMap.Builder<String, Table> builder) {

    }

    @Override
    protected CalciteSchema snapshot(@Nullable CalciteSchema parent, SchemaVersion version) {
        return null;
    }

    @Override
    protected boolean isCacheEnabled() {
        return false;
    }

    @Override
    public void setCache(boolean cache) {

    }

    @Override
    public CalciteSchema add(String name, Schema schema) {
        return null;
    }

    public io.dingodb.meta.entity.Table getTable(String tableName) {
        SubSnapshotSchema subSnapshotSchema = (SubSnapshotSchema) schema;
        return subSnapshotSchema.getTableInfo(tableName);
    }

}
