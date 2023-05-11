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

package io.dingodb.client.operation;

import com.google.common.base.Objects;

import java.util.Arrays;

public class Row<C extends Comparable<C>> implements Comparable<Row<C>> {

    private C column;
    private Object[] records;

    public Row(Object[] records, Comparable column) {
        this.records = records;
        this.column = (C) column;
    }

    public Object[] getRecords() {
        return records;
    }

    @Override
    public int compareTo(Row<C> that) {
        return this.column.compareTo(that.column);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        Row<?> row = (Row<?>) other;
        return Objects.equal(column, row.column);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(column);
    }

    @Override
    public String toString() {
        return "Row{"
            + "records=" + Arrays.toString(records)
            + '}';
    }
}
