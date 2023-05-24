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

import io.dingodb.client.OperationContext;
import io.dingodb.client.common.Key;
import io.dingodb.client.common.Record;
import io.dingodb.client.common.RouteTable;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;

public interface Operation {

    @Getter
    @EqualsAndHashCode
    @AllArgsConstructor
    class Task {
        private final DingoCommonId regionId;
        private final Any parameters;

        public <P> P parameters() {
            return parameters.getValue();
        }

    }

    @Getter
    @AllArgsConstructor
    class Fork {
        private final Object result;
        private final NavigableSet<Task> subTasks;
        private final boolean ignoreError;

        public <R> R result() {
            return (R) result;
        }
    }

    void exec(OperationContext context);

    Fork fork(Any parameters, Table table, RouteTable routeTable);

    Fork fork(OperationContext context, RouteTable routeTable);

    <R> R reduce(Fork context);

    default Object[] mapKey(Table table, Key key) {
        Object[] dst = new Object[table.getColumns().size()];
        return mapKey(key.getUserKey().toArray(), dst, table.getKeyColumns());
    }

    default Object[] mapKeyPrefix(Table table, Key key) {
        List<Column> keyColumns = table.getKeyColumns();
        keyColumns.sort(Comparator.comparingInt(Column::getPrimary));
        Object[] dst = new Object[table.getColumns().size()];
        Object[] src = key.getUserKey().toArray();
        return mapKey(src, dst, keyColumns.subList(0, src.length));
    }

    default Object[] mapKey(Object[] src, Object[] dst, List<Column> keyColumns) {
        if (keyColumns.size() != src.length) {
            throw new IllegalArgumentException(
                "Key column count is: " + keyColumns.size() + ", but give key count: " + src.length
            );
        }
        for (int i = 0; i < keyColumns.size(); i++) {
            Column column = keyColumns.get(i);
            dst[column.getPrimary()] = src[i];
            if (!column.isNullable() && dst[column.getPrimary()] == null) {
                throw new IllegalArgumentException("Non-null column [" + column.getName() + "] cannot be null");
            }
        }
        return dst;
    }

    default void checkParameters(List<Column> columns, Object[] record) {
        for (int i = 0; i < columns.size() && i < record.length; i++) {
            if (!columns.get(i).isNullable() && record[i] == null) {
                throw new IllegalArgumentException("Non-null column [" + columns.get(i).getName() + "] cannot be null");
            }
        }
    }

    default void checkParameters(Table table, Record record) {
        checkParameters(table.getColumns(), record.getValues().toArray());
    }
}
