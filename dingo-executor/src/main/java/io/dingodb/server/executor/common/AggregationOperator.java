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

package io.dingodb.server.executor.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class AggregationOperator implements io.dingodb.sdk.service.store.AggregationOperator {

    @AllArgsConstructor
    private static class AggregationType implements io.dingodb.sdk.service.store.AggregationOperator.AggregationType {

        private final io.dingodb.common.AggregationOperator.AggregationType type;

        public int getCode() {
            return type.getCode();
        }
    }

    public final io.dingodb.sdk.service.store.AggregationOperator.AggregationType operation;
    public final int indexOfColumn;

    public AggregationOperator(io.dingodb.common.AggregationOperator aggregation) {
        this.operation = new AggregationType(aggregation.operation);
        this.indexOfColumn = aggregation.indexOfColumn;
    }

}
