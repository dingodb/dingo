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

package io.dingodb.common.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.serial.schema.DingoSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@JsonTypeName("tuple")
@EqualsAndHashCode(of = {"fields"}, callSuper = true)
public class TupleType extends AbstractDingoType {
    @JsonProperty("fields")
    @Getter
    private final DingoType[] fields;

    @JsonCreator
    TupleType(
        @JsonProperty("fields") DingoType[] fields
    ) {
        super(TypeCode.TUPLE);
        this.fields = fields;
        setElementIds();
    }

    private void setElementIds() {
        for (int i = 0; i < fields.length; ++i) {
            fields[i].setId(i);
        }
    }

    @Override
    public Object convertValueTo(@Nonnull Object value, @Nonnull DataConverter converter) {
        Object[] tuple = (Object[]) value;
        checkFieldCount(tuple);
        return converter.collectTuple(
            IntStream.range(0, tuple.length)
                .mapToObj(i -> fields[i].convertTo(tuple[i], converter))
        );
    }

    @Override
    public Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter) {
        return checkFieldCount(converter.convertTupleFrom(value, this));
    }

    @Override
    public int fieldCount() {
        return fields.length;
    }

    @Override
    public DingoType getChild(@Nonnull Object index) {
        return fields[(int) index];
    }

    @Override
    public TupleType select(@Nonnull TupleMapping mapping) {
        DingoType[] newElements = new DingoType[mapping.size()];
        // Must do deep copying here
        mapping.revMap(newElements, fields, DingoType::copy);
        return new TupleType(newElements);
    }

    @Override
    public DingoType copy() {
        return new TupleType(
            Arrays.stream(fields)
                .map(DingoType::copy)
                .toArray(DingoType[]::new)
        );
    }

    @Nonnull
    @Override
    public Schema toAvroSchema() {
        return Schema.createRecord(
            getClass().getSimpleName(),
            null,
            getClass().getPackage().getName(),
            false,
            IntStream.range(0, fields.length)
                .mapToObj(i -> new Schema.Field("_" + i, fields[i].toAvroSchema()))
                .collect(Collectors.toList())
        );
    }

    @Override
    public List<DingoSchema> toDingoSchemas() {
        List<DingoSchema> schemas = new ArrayList<>(fields.length);
        for (int i = 0; i < fields.length; i++) {
            schemas.add(fields[i].toDingoSchema(i));
        }
        return schemas;
    }

    @Override
    public DingoSchema toDingoSchema(int index) {
        return null;
    }

    @Override
    public String format(@Nullable Object value) {
        if (value != null) {
            Object[] tuple = (Object[]) value;
            checkFieldCount(tuple);
            StringBuilder b = new StringBuilder();
            b.append("{ ");
            for (int i = 0; i < tuple.length; ++i) {
                if (i > 0) {
                    b.append(", ");
                }
                b.append(fields[i].format(tuple[i]));
            }
            b.append(" }");
            return b.toString();
        }
        return NullType.NULL.format(null);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[ ");
        for (int i = 0; i < fields.length; ++i) {
            if (i > 0) {
                b.append(", ");
            }
            b.append(fields[i]);
        }
        b.append(" ]");
        return b.toString();
    }

    @Nonnull
    private Object[] checkFieldCount(@Nonnull Object[] tuple) {
        if (tuple.length == fieldCount()) {
            return tuple;
        }
        throw new IllegalArgumentException(
            "Required " + fieldCount() + " elements, but " + tuple.length + " provided."
        );
    }
}
