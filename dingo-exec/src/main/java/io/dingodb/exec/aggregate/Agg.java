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

import io.dingodb.common.AggregationOperator;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface Agg {
    /**
     * Called when the first tuple arrived.
     *
     * @param tuple the tuple
     * @return the aggregating context, may be a compound type
     */
    Object first(@NonNull Object[] tuple);

    Object add(@NonNull Object var, @NonNull Object[] tuple);

    /**
     * Called to merge two aggregating context.
     *
     * @param var1 the 1st aggregating context, may be null
     * @param var2 the 2nd aggregating context, may be null
     * @return the merged aggregating context
     */
    Object merge(@Nullable Object var1, @Nullable Object var2);

    /**
     * Called to get the output value from aggregating context.
     *
     * @param var the aggregating context, may be null
     * @return the output value
     */
    Object getValue(@Nullable Object var);

    AggregationOperator.AggregationType getAggregationType();

    int getIndex();
}
