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
import org.checkerframework.checker.nullness.qual.NonNull;

public abstract class DingoTypeVisitorBase<R, T> implements DingoTypeVisitor<R, T> {
    public R visit(@NonNull DingoType type) {
        return type.accept(this, null);
    }

    public R visit(@NonNull DingoType type, T obj) {
        return type.accept(this, obj);
    }

    @Override
    public R visitNullType(@NonNull NullType type, T obj) {
        return null;
    }

    @Override
    public R visitIntegerType(@NonNull IntegerType type, T obj) {
        return null;
    }

    @Override
    public R visitLongType(@NonNull LongType type, T obj) {
        return null;
    }

    @Override
    public R visitFloatType(@NonNull FloatType type, T obj) {
        return null;
    }

    @Override
    public R visitDoubleType(@NonNull DoubleType type, T obj) {
        return null;
    }

    @Override
    public R visitBooleanType(@NonNull BooleanType type, T obj) {
        return null;
    }

    @Override
    public R visitDecimalType(@NonNull DecimalType type, T obj) {
        return null;
    }

    @Override
    public R visitStringType(@NonNull StringType type, T obj) {
        return null;
    }

    @Override
    public R visitBinaryType(@NonNull BinaryType type, T obj) {
        return null;
    }

    @Override
    public R visitDateType(@NonNull DateType type, T obj) {
        return null;
    }

    @Override
    public R visitTimeType(@NonNull TimeType type, T obj) {
        return null;
    }

    @Override
    public R visitTimestampType(@NonNull TimestampType type, T obj) {
        return null;
    }

    @Override
    public R visitObjectType(@NonNull ObjectType type, T obj) {
        return null;
    }

    @Override
    public R visitListType(@NonNull ListType type, T obj) {
        return null;
    }

    @Override
    public R visitMapType(@NonNull MapType type, T obj) {
        return null;
    }

    @Override
    public R visitTupleType(@NonNull TupleType type, T obj) {
        return null;
    }
}
