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

public interface SchemaConverter<S> {
    @NonNull S createSchema(@NonNull NullType type);

    @NonNull S createSchema(@NonNull IntegerType type);

    @NonNull S createSchema(@NonNull LongType type);

    @NonNull S createSchema(@NonNull FloatType type);

    @NonNull S createSchema(@NonNull DoubleType type);

    @NonNull S createSchema(@NonNull DecimalType type);

    @NonNull S createSchema(@NonNull StringType type);

    @NonNull S createSchema(@NonNull BooleanType type);

    @NonNull S createSchema(@NonNull BinaryType type);

    @NonNull S createSchema(@NonNull DateType type);

    @NonNull S createSchema(@NonNull TimeType type);

    @NonNull S createSchema(@NonNull TimestampType type);

    @NonNull S createSchema(@NonNull ObjectType type);

    @NonNull S createSchema(@NonNull TupleType type);

    @NonNull S createSchema(@NonNull ArrayType type);

    @NonNull S createSchema(@NonNull ListType type);

    @NonNull S createSchema(@NonNull MapType type);
}
