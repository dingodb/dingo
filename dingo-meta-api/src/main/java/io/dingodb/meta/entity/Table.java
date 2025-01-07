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

package io.dingodb.meta.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Getter
@ToString
@SuperBuilder
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Table {

    @JsonProperty
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    @EqualsAndHashCode.Include
    public final CommonId tableId;
    @JsonProperty
    @EqualsAndHashCode.Include
    public final String name;

    @JsonProperty
    public final List<Column> columns;

    @JsonProperty
    public final int replica;
    @JsonProperty
    public final String partitionStrategy;
    @JsonProperty
    public final List<Partition> partitions;

    @JsonProperty
    public final String engine;
    @JsonProperty
    public final int version;

    @JsonProperty
    public final Properties properties;

    @JsonProperty
    public final long autoIncrement;

    @JsonProperty
    public final String charset;
    @JsonProperty
    public final String collate;

    @JsonProperty
    public final String tableType;
    @JsonProperty
    public final String rowFormat;

    @JsonProperty
    public final long createTime;
    @JsonProperty
    public final long updateTime;

    @JsonProperty
    public final List<IndexTable> indexes;

    @Setter
    public Table replicaTable;

    @JsonProperty
    public final String comment;

    @JsonProperty
    public final String createSql;

    @JsonProperty
    public SchemaState schemaState;

    @JsonProperty
    public int codecVersion;

    public TupleType tupleType() {
        return DingoTypeFactory.tuple(columns.stream().map(Column::getType).toArray(DingoType[]::new));
    }

    public DingoType onlyKeyType() {
        return DingoTypeFactory.tuple(
            columns.stream()
                .filter(Column::isPrimary)
                .sorted(Comparator.comparingInt(Column::getPrimaryKeyIndex))
                .map(Column::getType)
                .toArray(DingoType[]::new)
        );
    }

    public @Nullable Column getColumn(String name) {
        for (Column column : columns) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column;
            }
        }
        return null;
    }

    public List<Column> keyColumns() {
        return columns.stream().filter(Column::isPrimary).collect(Collectors.toList());
    }

    public TupleMapping keyMapping() {
        int[] mappings = new int[columns.size()];
        int keyCount = 0;
        for (int i = 0; i < columns.size(); i++) {
            int primaryKeyIndex = columns.get(i).primaryKeyIndex;
            if (primaryKeyIndex >= 0) {
                mappings[primaryKeyIndex] = i;
                keyCount++;
            }
        }
        return TupleMapping.of(Arrays.copyOf(mappings, keyCount));
    }

    public TupleMapping mapping() {
        return TupleMapping.of(IntStream.range(0, columns.size()).toArray());
    }

    public int getColumnIndex(String name) {
        int i = 0;
        for (Column column : columns) {
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

    public Table copyWithColumns(List<Column> columns) {
        return Table.builder()
            .tableId(this.tableId)
            .columns(columns)
            .indexes(this.indexes)
            .autoIncrement(this.autoIncrement)
            .charset(this.charset)
            .collate(this.collate)
            .comment(this.comment)
            .createSql(this.createSql)
            .createTime(this.createTime)
            .engine(this.engine)
            .name(this.name)
            .partitions(this.partitions)
            .partitionStrategy(this.partitionStrategy)
            .properties(this.properties)
            .replica(this.replica)
            .rowFormat(this.rowFormat)
            .tableType(this.tableType)
            .updateTime(this.updateTime)
            .version(this.version)
            .codecVersion(this.codecVersion)
            .build();
    }

}
