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

package io.dingodb.client.common;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
public final class Filter {

    /**
     * Create contains filter for query on collection index.
     *
     * @param startKey   start primary key.
     * @param endKey     end primary key
     * @param columnName column name
     * @param value      column value expected value.
     * @return the records satisfy the filter conditions.
     */
    public static Filter equal(Value[] startKey, Value[] endKey, String columnName, Value value) {
        return new Filter(startKey, endKey, columnName, value, value);
    }

    /**
     * Create contains filter for query on collection index.
     *
     * @param startKey   start primary key.
     * @param endKey     end primary key
     * @param columnName column name
     * @param value      column value expected value.
     * @return the records satisfy the filter conditions.
     */
    public static Filter contains(Value[] startKey, Value[] endKey, String columnName, Value value) {
        return new Filter(startKey, endKey, columnName, value, value);
    }

    /**
     * query records from startKey to endKey, where column values is between begin and end values.
     *
     * @param startKey   user start key.
     * @param endKey     user end key.
     * @param columnName column name
     * @param begin      column start value(contains)
     * @param end        column end value(contains)
     * @return SubRecords satisfy the filter conditions.
     */
    public static Filter range(Value[] startKey, Value[] endKey, String columnName, Value begin, Value end) {
        return new Filter(startKey, endKey, columnName, begin, end);
    }

    @Getter
    private final Value[] startKey;

    @Getter
    private final Value[] endKey;

    @Getter
    private final String columnName;

    @Getter
    private final Value columnValueStart;

    @Getter
    private final Value columnValueEnd;


    private Filter(Value[] startKey,
                   Value[] endKey,
                   String columnName,
                   Value columnValueStart,
                   Value columnValueEnd) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.columnName = columnName;
        this.columnValueStart = columnValueStart;
        this.columnValueEnd = columnValueEnd;
    }

    @Override
    public String toString() {
        return "Filter {"
            + " startKey=" + Arrays.toString(startKey)
            + ", endKey=" + Arrays.toString(endKey)
            + ", columnName=" + columnName
            + ", columnValueStart=" + columnValueStart
            + ", columnValueEnd=" + columnValueEnd
            + "}";
    }
}
