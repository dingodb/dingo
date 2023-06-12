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

package io.dingodb.client.operation.impl;

import io.dingodb.sdk.service.store.AggregationOperator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
@AllArgsConstructor
public class KeyRangeCoprocessor {
    public final OpKeyRange opKeyRange;
    public final List<Aggregation> aggregations;
    public final List<String> groupBy;

    @Getter
    @Builder
    @AllArgsConstructor
    public static class Aggregation {
        public final AggType operation;
        public final String columnName;
        public final String alias;
    }

    public enum AggType implements AggregationOperator.AggregationType {
        SUM(1),
        COUNT(2),
        COUNT_WITH_NULL(3), COUNTWITHNULL(3),
        MAX(4),
        MIN(5),
        SUM_0(6), SUM0(6)
        ;

        private final int code;

        AggType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public String resultType(String originType) {
            if (this.code == COUNT.code || this.code == COUNT_WITH_NULL.code) {
                return "LONG";
            }
            return originType;
        }

    }
}
