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

package io.dingodb.exec.codec;

import io.dingodb.common.type.DingoTypeVisitorBase;
import io.dingodb.common.type.ListType;
import io.dingodb.common.type.MapType;
import io.dingodb.common.type.NullType;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.BinaryType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.ObjectType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import org.apache.avro.Schema;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AvroSchemaConverter extends DingoTypeVisitorBase<Schema, Void> {
    public static final AvroSchemaConverter INSTANCE = new AvroSchemaConverter();

    private AvroSchemaConverter() {
    }

    private static @NonNull Schema ofNullable(Schema schema, boolean nullable) {
        // Allow avro to encode `null`.
        return nullable ? Schema.createUnion(schema, Schema.create(Schema.Type.NULL)) : schema;
    }

    @Override
    public Schema visitNullType(@NonNull NullType type, Void obj) {
        return Schema.create(Schema.Type.NULL);
    }

    @Override
    public Schema visitIntegerType(@NonNull IntegerType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.INT), type.isNullable());
    }

    @Override
    public Schema visitLongType(@NonNull LongType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.LONG), type.isNullable());
    }

    @Override
    public Schema visitFloatType(@NonNull FloatType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.FLOAT), type.isNullable());
    }

    @Override
    public Schema visitDoubleType(@NonNull DoubleType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.DOUBLE), type.isNullable());
    }

    @Override
    public Schema visitBooleanType(@NonNull BooleanType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.BOOLEAN), type.isNullable());
    }

    @Override
    public Schema visitDecimalType(@NonNull DecimalType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.STRING), type.isNullable());
    }

    @Override
    public Schema visitStringType(@NonNull StringType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.STRING), type.isNullable());
    }

    @Override
    public Schema visitBinaryType(@NonNull BinaryType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.BYTES), type.isNullable());
    }

    @Override
    public Schema visitDateType(@NonNull DateType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.LONG), type.isNullable());
    }

    @Override
    public Schema visitTimeType(@NonNull TimeType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.LONG), type.isNullable());
    }

    @Override
    public Schema visitTimestampType(@NonNull TimestampType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.LONG), type.isNullable());
    }

    @Override
    public Schema visitObjectType(@NonNull ObjectType type, Void obj) {
        return ofNullable(Schema.create(Schema.Type.BYTES), type.isNullable());
    }

    @Override
    public Schema visitListType(@NonNull ListType type, Void obj) {
        return ofNullable(Schema.createArray(visit(type.getElementType())), type.isNullable());
    }

    @Override
    public Schema visitMapType(@NonNull MapType type, Void obj) {
        // TODO: keys can only be STRING.
        return ofNullable(Schema.createMap(visit(type.getValueType())), type.isNullable());
    }

    @Override
    public Schema visitTupleType(@NonNull TupleType type, Void obj) {
        return Schema.createRecord(
            type.getClass().getSimpleName(),
            null,
            type.getClass().getPackage().getName(),
            false,
            IntStream.range(0, type.fieldCount())
                .mapToObj(i -> new Schema.Field("_" + i, visit(type.getChild(i))))
                .collect(Collectors.toList())
        );
    }
}
