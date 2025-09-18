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
import io.dingodb.expr.common.type.DecimalType;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.serial.schema.DingoSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

@JsonTypeName("tuple")
@EqualsAndHashCode(of = {"fields"}, callSuper = true)
public class TupleType extends AbstractDingoType {
    @JsonProperty("fields")
    @Getter
    private final DingoType[] fields;

    @Getter
    private final Type type;

    // create table b(id int,age int);
    // When executing insert into tab select * from b on a table without a primary key,
    // the hidden primary key is found but the target table does not have the relevant schema.
    // In this case, the fieldCount and tuple. length are inconsistent, No need to check
    // example: TxnPartInsertOperator,TxnPartInsertIgnoreOperator
    public boolean checkFieldCount;

    @Override
    public void setCheckFieldCount(boolean checkFieldCount) {
        this.checkFieldCount = checkFieldCount;
    }

    @JsonCreator
    public TupleType(
        @JsonProperty("fields") DingoType[] fields
    ) {
        super();
        this.fields = fields;
        setElementIds();
        type = Types.tuple(Arrays.stream(fields).map(
            item -> {
                if(item instanceof io.dingodb.common.type.scalar.DecimalType) {
                    DecimalType decType =  Types.getDecimalType();
                    long precision = ((io.dingodb.common.type.scalar.DecimalType)item).getPrecision();
                    long scale = ((io.dingodb.common.type.scalar.DecimalType)item).getScale();
                    decType.setPrecision(precision);
                    decType.setScale(scale);
                    return decType;
                } else {
                    return item.getType();
                }
            }
        ).toArray(Type[]::new));
        this.checkFieldCount = true;
    }

    private void setElementIds() {
        for (int i = 0; i < fields.length; ++i) {
            fields[i].setId(i);
        }
    }

    @Override
    public Object convertValueTo(@NonNull Object value, @NonNull DataConverter converter) {
        Object[] tuple = (Object[]) value;
        checkFieldCount(tuple);
        return converter.collectTuple(
            IntStream.range(0, fieldCount())
                .filter(i -> !fields[i].isHidden())
                .mapToObj(i -> fields[i].convertTo(tuple[i], converter))
        );
    }

    @Override
    public Object convertValueFrom(@NonNull Object value, @NonNull DataConverter converter) {
        return checkFieldCount(converter.convertTupleFrom(value, this));
    }

    @Override
    public int fieldCount() {
        return fields.length;
    }

    @Override
    public DingoType getChild(@NonNull Object index) {
        int idx = (int) index;
        if (idx < fieldCount()) {
            return fields[(int) index];
        } else {
            // When executing insert into tab select * from without PrimaryTab on a table without a primary key,
            // the hidden primary key is found but the target table does not have the relevant schema
            // set type = null
            return null;
        }
    }

    @Override
    public @NonNull TupleType select(@NonNull TupleMapping mapping) {
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
    public @NonNull String format(@Nullable Object value) {
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
    public <R, T> R accept(@NonNull DingoTypeVisitor<R, T> visitor, T obj) {
        return visitor.visitTupleType(this, obj);
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

    private Object @NonNull [] checkFieldCount(Object @NonNull [] tuple) {
        if (checkFieldCount) {
            if (tuple.length == fieldCount()) {
                return tuple;
            } else {
                throw new IllegalArgumentException(
                    "Required " + fieldCount() + " elements, but " + tuple.length + " provided."
                );
            }
        } else {
            return tuple;
        }
    }
}
