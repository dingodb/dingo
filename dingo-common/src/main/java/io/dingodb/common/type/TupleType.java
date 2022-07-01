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
import com.fasterxml.jackson.annotation.JsonValue;
import io.dingodb.expr.runtime.TypeCode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

@EqualsAndHashCode(of = {"fields"}, callSuper = true)
class TupleType extends AbstractDingoType {
    @JsonValue
    @Getter
    private final DingoType[] fields;

    @JsonCreator
    public TupleType(DingoType[] fields) {
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
    public DingoType copy() {
        return new TupleType(
            Arrays.stream(fields)
                .map(DingoType::copy)
                .toArray(DingoType[]::new)
        );
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
    public String format(@Nonnull Object value) {
        Object[] tuple = (Object[]) value;
        checkFieldCount(tuple.length);
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

    @Override
    public Object convertValueTo(@Nonnull Object value, @Nonnull DataConverter converter) {
        Object[] tuple = (Object[]) value;
        checkFieldCount(tuple.length);
        return converter.collectTuple(
            IntStream.range(0, tuple.length)
                .mapToObj(i -> fields[i].convertTo(tuple[i], converter))
        );
    }

    @Override
    public Object convertValueFrom(@Nonnull Object value, @Nonnull DataConverter converter) {
        Object[] tuple = (Object[]) value;
        checkFieldCount(tuple.length);
        return IntStream.range(0, tuple.length)
            .mapToObj(i -> fields[i].convertFrom(tuple[i], converter))
            .toArray(Object[]::new);
    }

    private void checkFieldCount(int count) {
        if (count != fieldCount()) {
            throw new IllegalArgumentException(
                "Required " + fieldCount() + " elements, but " + count + " provided."
            );
        }
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
}
