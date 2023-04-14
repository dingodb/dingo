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

import io.dingodb.common.type.ArrayType;
import io.dingodb.common.type.ListType;
import io.dingodb.common.type.MapType;
import io.dingodb.common.type.NullType;
import io.dingodb.common.type.SchemaConverter;
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

public class AvroSchemaConverter implements SchemaConverter<Schema> {
    public static final AvroSchemaConverter INSTANCE = new AvroSchemaConverter();

    private AvroSchemaConverter() {
    }

    private static @NonNull Schema ofNullable(Schema schema, boolean nullable) {
        // Allow avro to encode `null`.
        return nullable ? Schema.createUnion(schema, Schema.create(Schema.Type.NULL)) : schema;
    }

    @Override
    public @NonNull Schema createSchema(@NonNull NullType type) {
        return Schema.create(Schema.Type.NULL);
    }

    @Override
    public @NonNull Schema createSchema(@NonNull IntegerType type) {
        return ofNullable(Schema.create(Schema.Type.INT), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull LongType type) {
        return ofNullable(Schema.create(Schema.Type.LONG), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull FloatType type) {
        return ofNullable(Schema.create(Schema.Type.DOUBLE), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull DoubleType type) {
        return ofNullable(Schema.create(Schema.Type.DOUBLE), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull DecimalType type) {
        return ofNullable(Schema.create(Schema.Type.STRING), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull StringType type) {
        return ofNullable(Schema.create(Schema.Type.STRING), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull BooleanType type) {
        return ofNullable(Schema.create(Schema.Type.BOOLEAN), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull BinaryType type) {
        return ofNullable(Schema.create(Schema.Type.BYTES), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull DateType type) {
        return ofNullable(Schema.create(Schema.Type.LONG), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull TimeType type) {
        return ofNullable(Schema.create(Schema.Type.LONG), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull TimestampType type) {
        return ofNullable(Schema.create(Schema.Type.LONG), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull ObjectType type) {
        return ofNullable(Schema.create(Schema.Type.BYTES), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull TupleType type) {
        return Schema.createRecord(
            type.getClass().getSimpleName(),
            null,
            type.getClass().getPackage().getName(),
            false,
            IntStream.range(0, type.fieldCount())
                .mapToObj(i -> new Schema.Field("_" + i, type.getChild(i).toSchema(this)))
                .collect(Collectors.toList())
        );
    }

    @Override
    public @NonNull Schema createSchema(@NonNull ArrayType type) {
        return ofNullable(Schema.createArray(type.getElementType().toSchema(this)), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull ListType type) {
        return ofNullable(Schema.createArray(type.getElementType().toSchema(this)), type.isNullable());
    }

    @Override
    public @NonNull Schema createSchema(@NonNull MapType type) {
        return ofNullable(Schema.createMap(type.getValueType().toSchema(this)), type.isNullable());
    }
}
