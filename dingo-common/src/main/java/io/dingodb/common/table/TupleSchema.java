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
import com.fasterxml.jackson.annotation.JsonValue;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.TypeCode;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

public class TupleSchema implements CompileContext {
    @JsonValue
    private final ElementSchema[] elementSchemas;

    @JsonCreator
    public TupleSchema(ElementSchema[] elementSchemas) {
        this.elementSchemas = elementSchemas;
        setElementIds();
    }

    @Nonnull
    public static TupleSchema fromRelDataType(@Nonnull RelDataType relDataType) {
        TupleSchema schema = new TupleSchema(
            relDataType.getFieldList().stream()
                .map(RelDataTypeField::getType)
                .map(ElementSchema::fromRelDataType)
                .toArray(ElementSchema[]::new)
        );
        schema.setElementIds();
        return schema;
    }

    @Nonnull
    public static TupleSchema ofTypes(int... typeCodes) {
        TupleSchema schema = new TupleSchema(
            Arrays.stream(typeCodes)
                .mapToObj(ElementSchema::new)
                .toArray(ElementSchema[]::new)
        );
        schema.setElementIds();
        return schema;
    }

    @Nonnull
    public static TupleSchema ofTypes(String... types) {
        TupleSchema schema = new TupleSchema(
            Arrays.stream(types)
                .map(ElementSchema::fromString)
                .toArray(ElementSchema[]::new)
        );
        schema.setElementIds();
        return schema;
    }

    public int size() {
        return elementSchemas.length;
    }

    private void setElementIds() {
        for (int i = 0; i < elementSchemas.length; ++i) {
            elementSchemas[i].setId(i);
        }
    }

    public ElementSchema get(int index) {
        return elementSchemas[index];
    }

    @Override
    public int getTypeCode() {
        return TypeCode.TUPLE;
    }

    @Override
    public CompileContext getChild(@Nonnull Object index) {
        return elementSchemas[(int) index];
    }

    public TupleSchema select(@Nonnull TupleMapping mapping) {
        ElementSchema[] newElements = new ElementSchema[mapping.size()];
        // Must do deep copying here
        mapping.revMap(newElements, elementSchemas, ElementSchema::copy);
        return new TupleSchema(newElements);
    }

    public Schema getAvroSchema() {
        List<Schema.Field> fieldList = new ArrayList<>(elementSchemas.length);
        for (int i = 0; i < elementSchemas.length; ++i) {
            // Avro does not support `$` in field name.
            fieldList.add(elementSchemas[i].getAvroSchemaField("_" + i));
        }
        return Schema.createRecord(
            getClass().getSimpleName(),
            null,
            getClass().getPackage().getName(),
            false,
            fieldList);
    }

    public Object[] parse(@Nonnull String[] row) {
        Object[] result = new Object[row.length];
        for (int i = 0; i < row.length; ++i) {
            result[i] = elementSchemas[i].parse(row[i]);
        }
        return result;
    }

    public Object[] convert(@Nonnull Object[] row) {
        Object[] result = new Object[row.length];
        for (int i = 0; i < row.length; ++i) {
            result[i] = elementSchemas[i].convert(row[i]);
        }
        return result;
    }

    public String formatTuple(@Nonnull Object[] tuple) {
        StringBuilder b = new StringBuilder();
        b.append("{");
        int i = 0;
        for (Object t : tuple) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(t).append(":").append(elementSchemas[i].toString());
            ++i;
        }
        b.append("}");
        return b.toString();
    }
}
