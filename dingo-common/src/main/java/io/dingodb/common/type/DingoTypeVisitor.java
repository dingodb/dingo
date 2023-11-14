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

public interface DingoTypeVisitor<R, T> {
    R visitNullType(@NonNull NullType type, T obj);

    R visitIntegerType(@NonNull IntegerType type, T obj);

    R visitLongType(@NonNull LongType type, T obj);

    R visitFloatType(@NonNull FloatType type, T obj);

    R visitDoubleType(@NonNull DoubleType type, T obj);

    R visitBooleanType(@NonNull BooleanType type, T obj);

    R visitDecimalType(@NonNull DecimalType type, T obj);

    R visitStringType(@NonNull StringType type, T obj);

    R visitBinaryType(@NonNull BinaryType type, T obj);

    R visitDateType(@NonNull DateType type, T obj);

    R visitTimeType(@NonNull TimeType type, T obj);

    R visitTimestampType(@NonNull TimestampType type, T obj);

    R visitObjectType(@NonNull ObjectType type, T obj);

    R visitListType(@NonNull ListType type, T obj);

    R visitMapType(@NonNull MapType type, T obj);

    R visitTupleType(@NonNull TupleType type, T obj);
}
