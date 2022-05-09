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
import io.dingodb.expr.json.runtime.Parser;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ColumnStrategy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@JsonPropertyOrder({"name", "columns"})
@EqualsAndHashCode
public class TableDefinition {
    private static final Parser PARSER = Parser.JSON;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("name")
    @Getter
    private final String name;
    @JsonProperty("columns")
    @Getter
    @Setter
    private List<ColumnDefinition> columns;

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

    @Nonnull
    public static TableDefinition fromRelDataType(String name, @Nonnull RelDataType relDataType) {
        TableDefinition td = new TableDefinition(name);
        relDataType.getFieldList().forEach(f ->
            td.addColumn(ColumnDefinition.fromRelDataType(f.getName(), f.getType()))
        );
        return td;
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

    @Nullable
    public ColumnDefinition getColumn(String name) {
        for (ColumnDefinition column : columns) {
            // `name` may be uppercase.
            if (column.getName().equalsIgnoreCase(name)) {
                return column;
            }
        }
        return null;
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

    public int[] getColumnIndices(@Nonnull List<String> names) {
        int[] indices = new int[names.size()];
        for (int i = 0; i < names.size(); ++i) {
            indices[i] = getColumnIndex(names.get(i));
        }
        return indices;
    }

    public int getColumnsCount() {
        return columns.size();
    }

    public RelDataType getRelDataType(@Nonnull RelDataTypeFactory typeFactory) {
        // make column name uppercase to adapt to calcite
        return typeFactory.createStructType(
            columns.stream().map(c -> c.getRelDataType(typeFactory)).collect(Collectors.toList()),
            columns.stream().map(ColumnDefinition::getName).map(String::toUpperCase).collect(Collectors.toList())
        );
    }

    public TupleMapping getKeyMapping() {
        return getColumnMapping(true);
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

    @Nonnull
    private TupleMapping getColumnMapping(boolean keyOrValue) {
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

    public Schema getAvroSchemaOfKey() {
        return getTupleSchema(true).getAvroSchema();
    }

    public Schema getAvroSchemaOfValue() {
        return getTupleSchema(false).getAvroSchema();
    }

    public Schema getAvroSchema() {
        return getTupleSchema().getAvroSchema();
    }

    @Nonnull
    public TupleSchema getTupleSchema() {
        return new TupleSchema(
            columns.stream()
                .map(ColumnDefinition::getElementType)
                .toArray(ElementSchema[]::new)
        );
    }

    @Nonnull
    public TupleSchema getTupleSchema(boolean keyOrValue) {
        return new TupleSchema(
            getColumnMapping(keyOrValue).stream()
                .mapToObj(columns::get)
                .map(ColumnDefinition::getElementType)
                .toArray(ElementSchema[]::new)
        );
    }

    public ColumnStrategy getColumnStrategy(int index) {
        return columns.get(index).getColumnStrategy();
    }

    public String toJson() throws JsonProcessingException {
        return PARSER.stringify(this);
    }

    public void writeJson(OutputStream os) throws IOException {
        PARSER.writeStream(os, this);
    }

    public String formatTuple(Object[] tuple) {
        StringBuilder b = new StringBuilder();
        b.append("{");
        int i = 0;
        for (ColumnDefinition column : columns) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(column.getName()).append(": ").append(tuple[i]);
            ++i;
        }
        b.append("}");
        return b.toString();
    }

    @Override
    public String toString() {
        try {
            return toJson();
        } catch (JsonProcessingException e) {
            throw new AssertionError(e);
        }
    }
}
