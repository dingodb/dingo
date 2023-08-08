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

package io.dingodb.test.utils;

import lombok.Getter;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nonnull;

public final class ResultSetUtils {
    private ResultSetUtils() {
    }

    @Nonnull
    public static Row getRow(@Nonnull ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int size = metaData.getColumnCount();
        Object[] row = new Object[size];
        for (int i = 0; i < size; ++i) {
            row[i] = resultSet.getObject(i + 1);
        }
        return new Row(row);
    }

    public static class Row {
        @Getter
        private final Object[] tuple;

        Row(Object... values) {
            tuple = values;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(tuple);
        }

        @Override
        public boolean equals(Object obj) {
            Object[] values;
            if (obj instanceof Row) {
                values = ((Row) obj).tuple;
            } else if (obj instanceof Object[]) {
                values = (Object[]) obj;
            } else {
                return false;
            }
            if (tuple.length != values.length) {
                return false;
            }
            for (int i = 0; i < tuple.length; ++i) {
                Object v = tuple[i];
                if (v instanceof Date || v instanceof Time) {
                    if (!v.toString().equals(values[i].toString())) {
                        return false;
                    }
                } else if (!Objects.equals(v, values[i])) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return Arrays.toString(tuple);
        }
    }
}
