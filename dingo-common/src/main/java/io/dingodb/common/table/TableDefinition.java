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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.expr.json.runtime.Parser;
import io.dingodb.serial.schema.DingoSchema;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@JsonPropertyOrder({"name", "columns", "ttl", "partition", "prop"})
@EqualsAndHashCode
@AllArgsConstructor
public class TableDefinition {
    private static final Parser PARSER = Parser.JSON;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("name")
    private final String name;

    @JsonProperty("columns")
    @Getter
    @Setter
    private List<ColumnDefinition> columns;

    @JsonProperty("indexes")
    @Getter
    @Setter
    private Map<String, Index> indexes = new ConcurrentHashMap<>();

    @JsonProperty("version")
    @Getter
    private int version;

    @JsonProperty("ttl")
    @Getter
    @Setter
    private int ttl;

    @JsonProperty("partition")
    @Getter
    @Setter
    private PartitionDefinition partDefinition;

    @JsonProperty("prop")
    @Getter
    @Setter
    private Properties properties;

    @JsonCreator
    public TableDefinition(@JsonProperty("name") String name) {
        this.name = name;
    }

    public static TableDefinition fromJson(String json) throws IOException {
        return PARSER.parse(json, TableDefinition.class);
    }

    public static TableDefinition readJson(InputStream is) throws IOException {
        return PARSER.parse(is, TableDefinition.class);
    }

    public String getName() {
        return name.toUpperCase();
    }

    public TableDefinition addColumn(ColumnDefinition column) {
        if (columns == null) {
            columns = new LinkedList<>();
        }
        this.columns.add(column);
        return this;
    }

    public ColumnDefinition getColumn(int index) {
        return columns.get(index);
    }

    public @Nullable ColumnDefinition getColumn(String name) {
        for (ColumnDefinition column : columns) {
            // `name` may be uppercase.
            if (column.getName().equalsIgnoreCase(name)) {
                return column;
            }
        }
        return null;
    }

    public int getColumnIndexOfValue(String name) {
        return getColumnIndex(name) - getPrimaryKeyCount();
    }

    public int getColumnIndex(String name) {
        int i = 0;
        for (ColumnDefinition column : columns) {
            // `name` may be uppercase.
            if (column.getName().equalsIgnoreCase(name)) {
                return i;
            }
            ++i;
        }
        return -1;
    }

    public int[] getColumnIndices(@NonNull List<String> names) {
        int[] indices = new int[names.size()];
        for (int i = 0; i < names.size(); ++i) {
            indices[i] = getColumnIndex(names.get(i));
        }
        return indices;
    }

    public int getPrimaryKeyCount() {
        int count = 0;
        for (ColumnDefinition column : columns) {
            if (column.isPrimary()) {
                count++;
            }
        }
        return count;
    }

    public int getColumnsCount() {
        return columns.size();
    }

    public TupleMapping getKeyMapping() {
        return getColumnMapping(true);
    }

    public TupleMapping getRevKeyMapping() {
        return getKeyMapping().reverse(getColumnsCount());
    }

    public TupleMapping getValueMapping() {
        return getColumnMapping(false);
    }

    public TupleMapping getMapping() {
        List<Integer> indices = new LinkedList<>();
        int index = 0;
        for (ColumnDefinition column : columns) {
            indices.add(index);
            ++index;
        }
        return TupleMapping.of(indices);
    }

    public int getFirstPrimaryColumnIndex() {
        int index = 0;
        for (ColumnDefinition column : columns) {
            if (column.isPrimary()) {
                return index;
            }
            ++index;
        }
        return -1;
    }

    private @NonNull TupleMapping getColumnMapping(boolean keyOrValue) {
        List<Integer> indices = new LinkedList<>();
        int index = 0;
        for (ColumnDefinition column : columns) {
            if (column.isPrimary() == keyOrValue) {
                indices.add(index);
            }
            ++index;
        }
        return TupleMapping.of(indices);
    }

    public List<DingoSchema> getDingoSchemaOfKey() {
        List<DingoSchema> keySchema = new ArrayList<>();
        int index = 0;
        for (ColumnDefinition column : columns) {
            if (column.isPrimary()) {
                keySchema.add(column.getType().toDingoSchema(index++));
            }
        }
        return keySchema;
    }

    public List<DingoSchema> getDingoSchemaOfValue() {
        List<DingoSchema> valueSchema = new ArrayList<>();
        int index = 0;
        for (ColumnDefinition column : columns) {
            if (!column.isPrimary()) {
                valueSchema.add(column.getType().toDingoSchema(index++));
            }
        }
        return valueSchema;
    }

    public List<DingoSchema> getDingoSchema() {
        List<DingoSchema> schema = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            schema.add(columns.get(i).getType().toDingoSchema(i));
        }
        return schema;
    }

    public DingoType getDingoType() {
        return DingoTypeFactory.tuple(
            columns.stream()
                .map(ColumnDefinition::getType)
                .toArray(DingoType[]::new)
        );
    }

    public DingoType getDingoType(boolean keyOrValue) {
        return DingoTypeFactory.tuple(
            getColumnMapping(keyOrValue).stream()
                .mapToObj(columns::get)
                .map(ColumnDefinition::getType)
                .toArray(DingoType[]::new)
        );
    }

    public DingoKeyValueCodec createCodec() {
        return new DingoKeyValueCodec(getDingoType(), getKeyMapping());
    }

    public String toJson() throws JsonProcessingException {
        return PARSER.stringify(this);
    }

    public void writeJson(OutputStream os) throws IOException {
        PARSER.writeStream(os, this);
    }

    @Override
    public String toString() {
        try {
            return toJson();
        } catch (JsonProcessingException e) {
            throw new AssertionError(e);
        }
    }

    public void addIndex(Index newIndex) {
        for (String columnName : newIndex.getColumns()) {
            if (getColumn(columnName) == null) {
                throw new IllegalArgumentException("Column " + columnName + " not found in table " + name);
            }
        }
        indexes.put(newIndex.getName(), newIndex);
    }

    public void setIndexNormal(String indexName) {
        setIndexStatus(indexName, IndexStatus.NORMAL);
    }

    public void setIndexBusy(String indexName) {
        setIndexStatus(indexName, IndexStatus.BUSY);
    }

    public void setIndexDeleted(String indexName) {
        setIndexStatus(indexName, IndexStatus.DELETED);
    }

    private void setIndexStatus(String indexName, IndexStatus status) {
        Index index = indexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException("Index " + indexName + " not found in table " + name);
        }
        index.setStatus(status);
    }

    public synchronized void increaseVersion() {
        version++;
    }


    public void deleteIndex(String indexName) {
        if (indexes.remove(indexName) == null) {
            throw new IllegalArgumentException("Index " + indexName + " not found in table " + name);
        }
    }

    public int getNonDeleteIndexesCount() {
        int count = 0;
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            if (entry.getValue().getStatus() != IndexStatus.DELETED) {
                count ++;
            }
        }
        return count;
    }

    public List<String> getBusyIndexes() {
        List<String> busyIndex = new ArrayList<>();
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            if (entry.getValue().getStatus() == IndexStatus.BUSY) {
                busyIndex.add(entry.getKey());
            }
        }
        return busyIndex;
    }

    public List<String> getDeletedIndexes() {
        List<String> deletedIndex = new ArrayList<>();
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            if (entry.getValue().getStatus() == IndexStatus.DELETED) {
                deletedIndex.add(entry.getKey());
            }
        }
        return deletedIndex;
    }

    public Index getIndex(String indexName) {
        if (!indexes.containsKey(indexName)) {
            throw new IllegalArgumentException("Index " + indexName + " not found in table " + name);
        }
        return indexes.get(indexName);
    }

    public List<Index> getIndexes(List<String> indexNames) {
        List<Index> indexes = new ArrayList<>();
        for (String indexName : indexNames) {
            indexes.add(getIndex(indexName));
        }
        return indexes;
    }

    public List<Index> getIndexesByContainsColumnName(String columnName) {
        return getIndexes(getIndexNamesByContainsColumnName(columnName));
    }

    public List<String> getIndexNamesByContainsColumnName(String columnName) {
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            for (String column : entry.getValue().getColumns()) {
                if (column.equalsIgnoreCase(columnName)) {
                    result.add(entry.getValue().getName());
                    break;
                }
            }
        }
        return result;
    }

    public List<Index> getIndexesByEqualsColumnNames(List<String> columnNames) {
        return getIndexes(getIndexNamesByEqualsColumnNames(columnNames));
    }



    public List<String> getIndexNamesByEqualsColumnNames(List<String> columnNames) {
        List<String> result = new ArrayList<>();
        NEXTINDEX:
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            String[] indexColumnNames = entry.getValue().getColumns();
            if (indexColumnNames.length != columnNames.size()) {
                continue;
            }
            for (String indexColumnName : indexColumnNames) {
                if (!columnNames.contains(indexColumnName)) {
                    continue NEXTINDEX;
                }
            }
            result.add(entry.getValue().getName());
        }
        return result;
    }

    public TupleMapping getIndexMapping(String indexName) {
        Index index = getIndex(indexName);
        List<Integer> indices = new LinkedList<>();
        for (String columnName : index.getColumns()) {
            indices.add(getColumnIndex(columnName));
        }
        return TupleMapping.of(indices);
    }

    public Map<String, TupleMapping> getIndexesMapping() {
        Map<String, TupleMapping> indexesMapping = new HashMap<>();
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            indexesMapping.put(entry.getKey(), getIndexMapping(entry.getKey()));
        }
        return indexesMapping;
    }

    public void removeIndex(String name) {
        indexes.remove(name);
    }
}
