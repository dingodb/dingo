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

package io.dingodb.meta;

import io.dingodb.common.CommonId;
import io.dingodb.common.ddl.ModifyingColInfo;
import io.dingodb.common.ddl.RecoverInfo;
import io.dingodb.common.meta.SchemaInfo;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.sequence.SequenceDefinition;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;

import java.util.List;

public interface DdlService {
    static DdlService root() {
        return DdlServiceProvider.getDefault().root();
    }

    void createSchema(String schemaName, long schemaId, String connId);

    void dropSchema(SchemaInfo schemaInfo, String connId);

    void createTableWithInfo(String schemaName,
                             TableDefinition tableDefinition, String connId, String sql);

    default void createViewWithInfo(String schemaName,
                             TableDefinition tableDefinition, String connId, String sql) {

    }

    void dropTable(SchemaInfo schemaInfo, Long tableId, String tableName, String connId);

    void truncateTable(SchemaInfo schemaInfo, Table table, String connId);

    default void addColumn(SchemaInfo schemaInfo, Table table, ColumnDefinition column, String connId) {

    }

    default void dropColumn(
        long schemaId, String schemaName,
        Long tableId, String tableName, String column,
        String markDel, String relatedIndex, String connId
    ) {

    }


    default void modifyColumn(
        long schemaId, String schemaName, long tableId, List<ModifyingColInfo> modifyingColInfoList
    ) {

    }

    default void changeColumn(
        long schemaId, String schemaName, long tableId, ModifyingColInfo modifyingColInfo
    ) {

    }

    default void renameTable(long schemaId, String schemaName, long tableId, String tableName, String toName) {

    }

    default void renameIndex(long schemaId, String schemaName, long tableId, String tableName,
        String originIndexName, String toIndexName) {

    }

    default void alterModifyComment(long schemaId, String schemaName, long tableId, String tableName, String comment) {

    }

    default void createIndex(String schemaName, String tableName, TableDefinition indexDef) {

    }

    default void dropIndex(String schemaName, String tableName, String indexName) {

    }

    InfoSchema getIsLatest();

    default InfoSchema getPointIs(long pointTs) {
        return new InfoSchema();
    }

    Table getTable(String schemaName, String tableName);

    Table getTable(CommonId id);

    default void recoverTable(RecoverInfo recoverInfo) {

    }

    default void recoverSchema(RecoverInfo recoverInfo) {

    }

    void createSequence(SequenceDefinition sequenceDefinition, String connId);

    void dropSequence(String sequenceName, String connId);

    default void rebaseAutoInc(String schemaName, String tableName, long tableId, long autoInc) {

    }

    default void resetAutoInc() {

    }

    default void alterIndexVisible(
        long schemaId, String schemaName, long tableId, String tableName, String index, boolean invisible
    ) {

    }

    default void alterTableAddPart(
        long schemaId, String schemaName, long tableId, String tableName, PartitionDetailDefinition part
    ) {

    }

    default void alterTableDropPart(long schemaId, String schemaName, long tableId, String tableName, String part) {

    }

    default void alterTableTruncatePart(long schemaId, String schemaName, long tableId, String tableName, String part) {

    }
}
