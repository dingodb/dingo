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

package io.dingodb.exec.aggregate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.AggregationOperator;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@JsonTypeName("count")
public class CountAgg extends UnityAgg {
    @JsonCreator
    public CountAgg(
        @JsonProperty("index") Integer index
    ) {
        super(index);
    }

    static Object countMerge(@Nullable Object var1, @Nullable Object var2) {
        if (var1 != null) {
            if (var2 != null) {
                return (long) var1 + (long) var2;
            }
            return var1;
        }
        return var2;
    }

    @Override
    public Object first(Object @NonNull [] tuple) {
        Object value = tuple[index];
        return value != null ? 1L : null;
    }

    @Override
    public Object add(@NonNull Object var, Object @NonNull [] tuple) {
        Object value = tuple[index];
        if (value != null) {
            return (long) var + 1L;
        }
        return var;
    }

    @Override
    public Object merge(@Nullable Object var1, @Nullable Object var2) {
        return countMerge(var1, var2);
    }

    @Override
    public Object getValue(@Nullable Object var) {
        return var != null ? var : 0L;
    }

    @Override
    public AggregationOperator.AggregationType getAggregationType() {
        return AggregationOperator.AggregationType.COUNT;
    }
}
