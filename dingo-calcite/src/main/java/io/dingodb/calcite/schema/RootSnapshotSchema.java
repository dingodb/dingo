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
import io.dingodb.common.CommonId;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.entity.InfoSchema;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class RootSnapshotSchema implements Schema {
    public static final String ROOT_SCHEMA_NAME = "DINGO_ROOT";
    public static final String DEFAULT_SCHEMA_NAME = "DINGO";
    @Getter
    protected InfoSchema is;
    CommonId txnId;
    @Getter
    protected final DingoParserContext context;
    @Getter
    protected final List<String> names;

    public RootSnapshotSchema(DingoParserContext context) {
        //DdlService ddlService = DdlService.root();
        //this.is = ddlService.getIsLatest();
        this.context = context;
        this.names = ImmutableList.of(ROOT_SCHEMA_NAME);
    }

    public RootSnapshotSchema(InfoSchema is, DingoParserContext context, List<String> names) {
        this.is = is;
        this.context = context;
        this.names = names;
    }

    public void initTxn(InfoSchema is, CommonId txnId) {
        if (is == null) {
            DdlService ddlService = DdlService.root();
            this.is = ddlService.getIsLatest();
        } else {
            this.is = is;
        }
        this.txnId = txnId;
    }

    public void destoryTxn() {
        this.is = null;
        this.txnId = null;
    }

    @Override
    public @Nullable Table getTable(String s) {
        return null;
    }

    @Override
    public Set<String> getTableNames() {
        return new HashSet<>();
    }

    @Override
    public @Nullable RelProtoDataType getType(String s) {
        return null;
    }

    @Override
    public Set<String> getTypeNames() {
        return ImmutableSet.of();
    }

    @Override
    public Collection<Function> getFunctions(String s) {
        return ImmutableSet.of();
    }

    @Override
    public Set<String> getFunctionNames() {
        return ImmutableSet.of();
    }

    @Override
    public @Nullable SubSnapshotSchema getSubSchema(String schemaName) {
        if (is == null) {
            InfoSchema isTmp = DdlService.root().getIsLatest();
            if (isTmp == null) {
                return null;
            }
            return getSubSchema(isTmp, schemaName);
        }
        return getSubSchema(is, schemaName);
    }

    public SubSnapshotSchema getSubSchema(InfoSchema isTmp, String schemaName) {
        if (!isTmp.schemaMap.containsKey(schemaName)) {
            return null;
        }
        return new SubSnapshotSchema(isTmp, schemaName, context, ImmutableList.of(ROOT_SCHEMA_NAME, schemaName));
    }

    @Override
    public Set<String> getSubSchemaNames() {
        if (this.is == null) {
            InfoSchema isTmp = DdlService.root().getIsLatest();
            if (isTmp == null) {
                return new HashSet<>();
            }
            return isTmp.getSchemaMap().keySet();
        }
        return this.is.getSchemaMap().keySet();
    }

    @Override
    public Expression getExpression(@Nullable SchemaPlus schemaPlus, String name) {
        requireNonNull(schemaPlus, "parentSchema");
        return Schemas.subSchemaExpression(schemaPlus, name, getClass());
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Schema snapshot(SchemaVersion schemaVersion) {
        return this;
    }

}
