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

package io.dingodb.client.utils;

import io.dingodb.client.common.Key;
import io.dingodb.client.common.Record;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.dingodb.common.util.Parameters.cleanNull;

public final class OperationUtils {

    private OperationUtils() {
    }

    public static Object[] mapKey(Table table, Key key) {
        List<Column> columns = table.getColumns();
        List<Column> keyColumns = table.getKeyColumns();
        Object[] dst = new Object[columns.size()];
        Object[] src = key.getUserKey().toArray();
        if (key.columnOrder) {
            return mapKey(src, dst, columns, keyColumns);
        } else {
            return mapKey(src, dst, columns, CodecUtils.sortColumns(keyColumns));
        }
    }

    public static Object[] mapKey(Object[] src, Object[] dst, List<Column> columns, List<Column> keyColumns) {
        if (keyColumns.size() != src.length) {
            throw new IllegalArgumentException(
                "Key column count is: " + keyColumns.size() + ", but give key count: " + src.length
            );
        }
        for (int i = 0; i < keyColumns.size(); i++) {
            Column column = keyColumns.get(i);
            if ((dst[columns.indexOf(column)] = src[i]) == null && !column.isNullable()) {
                throw new IllegalArgumentException("Non-null column [" + column.getName() + "] cannot be null");
            }
        }
        return dst;
    }

    public static Object[] mapKey2(
        Object[] src,
        Object[] dst,
        List<io.dingodb.meta.entity.Column> columns,
        List<io.dingodb.meta.entity.Column> keyColumns
    ) {
        if (keyColumns.size() != src.length) {
            throw new IllegalArgumentException(
                "Key column count is: " + keyColumns.size() + ", but give key count: " + src.length
            );
        }
        for (int i = 0; i < keyColumns.size(); i++) {
            io.dingodb.meta.entity.Column column = keyColumns.get(i);
            if ((dst[columns.indexOf(column)] = src[i]) == null && !column.isNullable()) {
                throw new IllegalArgumentException("Non-null column [" + column.getName() + "] cannot be null");
            }
        }
        return dst;
    }

    public static Object[] mapKeyPrefix(Table table, Key key) {
        List<Column> columns = table.getColumns();
        List<Column> keyColumns = table.getKeyColumns();
        Object[] dst = new Object[columns.size()];
        Object[] src = key.getUserKey().toArray();
        if (key.columnOrder) {
            throw new IllegalArgumentException("Key prefix not support column order key.");
        } else {
            return mapKey(src, dst, columns, CodecUtils.sortColumns(keyColumns).subList(0, src.length));
        }
    }

    public static Object[] mapKeyPrefix(io.dingodb.meta.entity.Table table, Key key) {
        List<io.dingodb.meta.entity.Column> columns = table.getColumns();
        List<io.dingodb.meta.entity.Column> keyColumns = table.keyColumns();
        Object[] dst = new Object[columns.size()];
        Object[] src = key.getUserKey().toArray();
        if (key.columnOrder) {
            throw new IllegalArgumentException("Key prefix not support column order key.");
        } else {
            return mapKey2(src, dst, columns, sortColumns(keyColumns).subList(0, src.length));
        }
    }

    public static List<io.dingodb.meta.entity.Column> sortColumns(List<io.dingodb.meta.entity.Column> columns) {
        List<io.dingodb.meta.entity.Column> codecOrderColumns = new ArrayList<>(columns);
        codecOrderColumns.sort(sortColumnByPrimaryComparator());
        return codecOrderColumns;
    }

    public static int compareColumnByPrimary(int c1, int c2) {
        if (c1 * c2 > 0) {
            return Integer.compare(c1, c2);
        }
        return c1 < 0 ? 1 : c2 < 0 ? -1 : c1 - c2;
    }

    public static Comparator<io.dingodb.meta.entity.Column> sortColumnByPrimaryComparator() {
        return (c1, c2) -> compareColumnByPrimary(c1.getPrimaryKeyIndex(), c2.getPrimaryKeyIndex());
    }

    public static void checkParameters(List<Column> columns, Object[] record) {
        for (int i = 0; i < columns.size() && i < record.length; i++) {
            if (!columns.get(i).isNullable() && record[i] == null) {
                throw new IllegalArgumentException("Non-null column [" + columns.get(i).getName() + "] cannot be null");
            }
        }
    }

    public static void checkParameters(Table table, Record record) {
        checkParameters(
            table.getColumns(),
            record.extractValues(table.getColumns().stream().map(Column::getName).collect(Collectors.toList())));
    }

    public static void checkParameters(Table table, Object[] record) {
        checkParameters(table.getColumns(), record);
    }

    public static Throwable getCause(Throwable err) {
        if (err instanceof CompletionException || err instanceof ExecutionException) {
            return cleanNull(err.getCause(), err);
        }
        return err;
    }

}
