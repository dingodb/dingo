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

package io.dingodb.common.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.partition.PartitionDefinition;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Properties;

public class IndexDefinition extends TableDefinition {
    @Getter
    @Setter
    private boolean unique;

    @Getter
    private List<String> originKeyList;

    @Getter
    private List<String> originWithKeyList;

    @JsonCreator
    public IndexDefinition(@JsonProperty("name") String name) {
        super(name);
    }

    public IndexDefinition(
        String name,
        List<ColumnDefinition> columns,
        int version,
        int ttl,
        PartitionDefinition partDefinition,
        String engine,
        Properties properties,
        long autoIncrement,
        int replica,
        String createSql,
        String comment,
        String charset,
        String collate,
        String tableType, String rowFormat, long createTime, long updateTime, SchemaState schemaState,
        List<IndexDefinition> indices, long prepareTableId,
        boolean unique,
        List<String> originKeyList,
        List<String> originWithKeyList,
        int codecVersion
    ) {
        super(name, columns, version, ttl, partDefinition, engine, properties, autoIncrement, replica, createSql,
            comment, charset, collate, tableType, rowFormat, createTime, updateTime, schemaState,
            indices, prepareTableId, true, codecVersion);
        this.unique = unique;
        this.originKeyList = originKeyList;
        this.originWithKeyList = originWithKeyList;
    }

    public static IndexDefinition createIndexDefinition(
        String name,
        TableDefinition definition,
        boolean unique,
        List<String> originKeyList,
        List<String> originWithKeyList
    ) {
        return new IndexDefinition(
            name,
            definition.getColumns(),
            definition.getVersion(),
            definition.getTtl(),
            definition.getPartDefinition(),
            definition.getEngine(),
            definition.getProperties(),
            definition.getAutoIncrement(),
            definition.getReplica(),
            null,
            definition.getComment(),
            definition.getCharset(),
            definition.getCollate(),
            definition.getTableType(),
            definition.getRowFormat(),
            definition.getCreateTime(),
            definition.getUpdateTime(),
            definition.getSchemaState(),
            definition.getIndices(),
            definition.getPrepareTableId(),
            unique,
            originKeyList,
            originWithKeyList,
            2
        );
    }
}
