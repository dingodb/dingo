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
import io.dingodb.common.CommonId;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.entity.InfoSchema;
import lombok.Builder;
import lombok.Getter;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class RootCalciteSchema extends CalciteSchema {

    @Getter
    private Map<Long, Long> relatedTableForMdl = new ConcurrentHashMap<>();

    @Builder
    protected RootCalciteSchema(
        CalciteSchema parent,
        RootSnapshotSchema schema,
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
    }

    @Override
    protected @Nullable CalciteSchema getImplicitSubSchema(String schemaName, boolean caseSensitive) {
        String name = schemaName.toUpperCase();
        Schema subSchema = schema.getSubSchema(name);
        if (subSchema == null) {
            return null;
        }
        return SubCalciteSchema.builder().rootCalciteSchema(this).schema(subSchema).name(schemaName).build();
    }

    @Override
    protected @Nullable TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
        return null;
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
        RootSnapshotSchema rootSchema = (RootSnapshotSchema) schema;
        Set<String> subSchemaNames = schema.getSubSchemaNames();
        subSchemaNames.forEach(name -> {
            SubSnapshotSchema subSnapshotSchema = new SubSnapshotSchema(
                rootSchema.is, name, rootSchema.context, ImmutableList.of(RootSnapshotSchema.ROOT_SCHEMA_NAME, name)
            );
            builder.put(name, SubCalciteSchema.builder().rootCalciteSchema(this).name(name).schema(subSnapshotSchema).build());
        });
    }

    @Override
    public void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {
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
        return true;
    }

    @Override
    public void setCache(boolean cache) {

    }

    @Override
    public CalciteSchema add(String name, Schema schema) {
        return null;
    }

    public InfoSchema initTxn(CommonId txnId) {
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) schema;
        DdlService ddlService = DdlService.root();
        InfoSchema is = ddlService.getIsLatest();
        rootSnapshotSchema.initTxn(is, txnId);
        return is;
    }

    public void closeTxn() {
        RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) schema;
        rootSnapshotSchema.destoryTxn();
        this.relatedTableForMdl.clear();
    }

    public void putRelatedTable(long tableId, long ver) {
        this.relatedTableForMdl.put(tableId, ver);
    }

    public void removeRelatedTable(long tableId) {
        this.relatedTableForMdl.remove(tableId);
    }
}
