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
import org.apache.calcite.schema.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Set;

public class SnapshotSchema extends RootSnapshotSchema {
    String schemaName;

    public SnapshotSchema(InfoSchema is, String schemaName) {
        super(is);
        this.schemaName = schemaName;
    }

    @Override
    public @Nullable Table getTable(String s) {
        return super.getTable(s);
    }

    @Override
    public Set<String> getTableNames() {
        return super.getTableNames();
    }
}
