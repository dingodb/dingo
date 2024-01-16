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
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.expr.json.runtime.Parser;
import io.dingodb.serial.schema.DingoSchema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Builder
@JsonPropertyOrder({"name", "columns", "ttl", "partition", "prop", "engine"})
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

    @JsonProperty("engine")
    @Getter
    @Setter
    private String engine;

    @JsonProperty("prop")
    @Getter
    @Setter
    private Properties properties;

    @JsonProperty("autoIncrement")
    @Getter
    private long autoIncrement = 1;
    @JsonProperty("replica")
    @Getter
    @Setter
    private int replica;
    @JsonProperty("createSql")
    @Getter
    @Setter
    private String createSql;
    @Getter
    @Setter
    private String comment;
    @Setter
    @Getter
    private String charset;
    @Getter
    @Setter
    private String collate;

    @Getter
    @Setter
    private String tableType;

    @Getter
    @Setter
    private String rowFormat;

    @Getter
    @Setter
    private long createTime;

    @Getter
    @Setter
    private long updateTime;

    @JsonCreator
    public TableDefinition(@JsonProperty("name") String name) {
        this.name = name;
    }

    public static TableDefinition fromJson(String json) throws IOException {
        return PARSER.parse(json, TableDefinition.class);
    }

    public static TableDefinition readJson(InputStream is) throws IOException {
        TableDefinition tableDefinition = PARSER.parse(is, TableDefinition.class);
        tableDefinition.getColumns().forEach(columnDefinition -> {
            columnDefinition.setState(1);
        });
        return tableDefinition;
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

    public List<Integer> getColumnIndices(@NonNull List<String> names) {
        return names.stream()
            .map(this::getColumnIndex)
            .collect(Collectors.toList());
    }

    public int[] getColumnIndices(@NonNull String @NonNull [] names) {
        int[] result = new int[names.length];
        for (int i = 0; i < names.length; ++i) {
            result[i] = getColumnIndex(names[i]);
        }
        return result;
    }

    private @NonNull List<Integer> getColumnIndices(boolean keyOrValue) {
        List<Integer> indices = new LinkedList<>();
        int index = 0;
        for (ColumnDefinition column : columns) {
            if (column.isPrimary() == keyOrValue) {
                indices.add(index);
            }
            ++index;
        }
        if (keyOrValue) {
            Integer[] pkIndices = new Integer[indices.size()];
            for (int i = 0; i < indices.size(); i++) {
                pkIndices[columns.get(indices.get(i)).getPrimary()] = indices.get(i);
            }
            return Arrays.asList(pkIndices);
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
        return TupleMapping.of(getKeyColumnIndices());
    }

    public TupleMapping getRevKeyMapping() {
        return getKeyMapping().reverse(getColumnsCount());
    }

    public TupleMapping getValueMapping() {
        return getColumnMapping(false);
    }

    public TupleMapping getMapping() {
        return TupleMapping.of(IntStream.range(0, columns.size()).toArray());
    }

    public int getFirstPrimaryColumnIndex() {
        int index = 0;
        for (ColumnDefinition column : columns) {
            if (column.getPrimary() == 0) {
                return index;
            }
            ++index;
        }
        return -1;
    }

    private @NonNull TupleMapping getColumnMapping(boolean keyOrValue) {
        return TupleMapping.of(getColumnIndices(keyOrValue));
    }

    public @NonNull List<Integer> getKeyColumnIndices() {
        return getColumnIndices(true);
    }

    public List<ColumnDefinition> getKeyColumns() {
        List<ColumnDefinition> keyCols = new LinkedList<>();
        for (ColumnDefinition column : columns) {
            if (column.isPrimary()) {
                keyCols.add(column);
            }
        }
        return keyCols;
    }

    public DingoType getKeyType() {
        return DingoTypeFactory.tuple(
            columns.stream()
                .filter(ColumnDefinition::isPrimary)
                .sorted(Comparator.comparingInt(ColumnDefinition::getPrimary))
                .map(ColumnDefinition::getType)
                .toArray(DingoType[]::new)
        );
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

    public TableDefinition copyWithName(String name) {
        TableDefinition tableDefinition = new TableDefinition(
            name,
            this.columns,
            this.version,
            this.ttl,
            this.partDefinition,
            this.engine,
            this.properties,
            this.autoIncrement,
            this.replica,
            null,
            this.comment,
            this.charset,
            this.collate,
            this.tableType,
            this.rowFormat,
            this.createTime,
            this.updateTime
        );
        return tableDefinition;
    }
}
