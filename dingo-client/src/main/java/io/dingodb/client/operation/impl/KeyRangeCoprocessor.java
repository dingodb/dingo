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

import java.util.Arrays;
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

    private static final List<String> supportNums = Arrays.asList(
        "BIGINT", "LONG", "INT", "INTEGER", "TINYINT", "FLOAT", "DOUBLE", "REAL", "DECIMAL"
    );

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

        public boolean checkType(String type) {
            switch (code) {
                case 1:
                    return isNum(type);
                case 2:
                case 3:
                    return true;
                case 4:
                case 5:
                    return isComparable(type);
                case 6:
                    return isNum(type);
                default:
                    throw new IllegalStateException("Unexpected value: " + code);
            }
        }

        private static boolean isNum(String type) {
            switch (type.toUpperCase()) {
                case "INT":
                case "INTEGER":
                case "TINYINT":
                case "LONG":
                case "BIGINT":
                case "FLOAT":
                case "DOUBLE":
                case "REAL":
                case "DECIMAL":
                    return true;
                default:
                    return false;
            }
        }

        private static boolean isComparable(String type) {
            switch (type.toUpperCase()) {
                case "INT":
                case "INTEGER":
                case "TINYINT":
                case "LONG":
                case "BIGINT":
                case "FLOAT":
                case "DOUBLE":
                case "REAL":
                case "DECIMAL":
                case "STRING":
                case "CHAR":
                case "VARCHAR":
                case "DATE":
                case "TIME":
                case "TIMESTAMP":
                case "BINARY":
                case "BYTES":
                case "VARBINARY":
                case "BLOB":
                    return true;
                default:
                    return false;
            }
        }


    }
}
