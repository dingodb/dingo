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
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoTable;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.SchemaTables;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SubSnapshotSchema extends RootSnapshotSchema {
    @Getter
    String schemaName;

    public SubSnapshotSchema(InfoSchema is, String schemaName, DingoParserContext context, List<String> names) {
        super(is, context, names);
        this.schemaName = schemaName;
    }

    @Override
    public @Nullable DingoTable getTable(String tableName) {
        SchemaTables schemaTables;
        if (is == null) {
            InfoSchema isTmp = DdlService.root().getIsLatest();
            if (isTmp == null) {
                return null;
            }
            schemaTables = isTmp.schemaMap.get(schemaName);
        } else {
            schemaTables = is.schemaMap.get(schemaName);;
        }
        if (schemaTables == null) {
            return null;
        }
        Table table = schemaTables.getTables().get(tableName);
        if (table == null) {
            return null;
        }
        return new DingoTable(
            context,
            ImmutableList.<String>builder().addAll(names).add(tableName).build(),
            null,
            table
        );
    }

    @Override
    public Set<String> getTableNames() {
        SchemaTables schemaTables;
        if (this.is == null) {
            InfoSchema isTmp = DdlService.root().getIsLatest();
            if (isTmp == null) {
                return new HashSet<>();
            }
            schemaTables = isTmp.getSchemaMap().get(schemaName);
        } else {
            schemaTables = this.is.getSchemaMap().get(schemaName);
        }
        if (schemaTables == null) {
            return new HashSet<>();
        }
        return schemaTables.getTables().keySet();
    }

    @Override
    public Schema snapshot(SchemaVersion schemaVersion) {
        return this;
    }

    public Table getTableInfo(String tableName) {
        SchemaTables schemaTables;
        if (is == null) {
            InfoSchema isTmp = DdlService.root().getIsLatest();
            if (isTmp == null) {
                return null;
            }
            schemaTables = isTmp.getSchemaMap().get(schemaName);
        } else {
            schemaTables = is.schemaMap.get(schemaName);
        }
        if (schemaTables == null) {
            return null;
        }
        return schemaTables.getTables().get(tableName);
    }

    public long getSchemaVer() {
        if (is == null) {
            InfoSchema isTmp = DdlService.root().getIsLatest();
            if (isTmp == null) {
                return 0;
            }
            return isTmp.getSchemaMetaVersion();
        }
        return is.schemaMetaVersion;
    }

    public SchemaInfo getSchemaInfo(String schemaName) {
        InfoSchema isLocal;
        if (is == null) {
            isLocal = DdlService.root().getIsLatest();
            if (isLocal == null) {
                return null;
            }
        } else {
            isLocal = this.is;
        }
        if (!isLocal.getSchemaMap().containsKey(schemaName)) {
            return null;
        }
        return isLocal.getSchemaMap().get(schemaName).getSchemaInfo();
    }

    public boolean inTransaction() {
        return this.is != null;
    }

}
