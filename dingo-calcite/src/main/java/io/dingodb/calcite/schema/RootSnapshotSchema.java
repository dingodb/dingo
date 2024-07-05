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

import io.dingodb.common.meta.InfoSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Set;

public class RootSnapshotSchema implements Schema {
    InfoSchema is;

    public RootSnapshotSchema(InfoSchema is) {
        this.is = is;
    }

    @Override
    public @Nullable Table getTable(String s) {
        return null;
    }

    @Override
    public Set<String> getTableNames() {
        return null;
    }

    @Override
    public @Nullable RelProtoDataType getType(String s) {
        return null;
    }

    @Override
    public Set<String> getTypeNames() {
        return null;
    }

    @Override
    public Collection<Function> getFunctions(String s) {
        return null;
    }

    @Override
    public Set<String> getFunctionNames() {
        return null;
    }

    @Override
    public @Nullable Schema getSubSchema(String s) {
        return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return null;
    }

    @Override
    public Expression getExpression(@Nullable SchemaPlus schemaPlus, String s) {
        return null;
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Schema snapshot(SchemaVersion schemaVersion) {
        return null;
    }
}
