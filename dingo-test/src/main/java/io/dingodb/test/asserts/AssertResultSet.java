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

package io.dingodb.test.asserts;

import io.dingodb.test.utils.ResultSetUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

import static java.lang.Math.abs;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public final class AssertResultSet {
    private final ResultSet instance;
    private ResultSetCheckConfig config;

    AssertResultSet(ResultSet obj) {
        instance = obj;
        config = new ResultSetCheckConfig();
    }

    private @NonNull Row getRow() throws SQLException {
        Object[] tuple = ResultSetUtils.getRow(instance);
        return new Row(tuple);
    }

    public AssertResultSet config(ResultSetCheckConfig config) {
        this.config = config;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public AssertResultSet isPlan() throws SQLException {
        assertThat(instance.getMetaData().getColumnCount()).isEqualTo(1);
        assertThat(instance.getMetaData().getColumnName(1)).isEqualTo("PLAN");
        int rowCount = 0;
        while (instance.next()) {
            ++rowCount;
        }
        assertThat(rowCount).isEqualTo(1);
        return this;
    }

    public AssertResultSet columnLabels(@Nonnull String[] labels) throws SQLException {
        ResultSetMetaData metaData = instance.getMetaData();
        for (int i = 0; i < labels.length; ++i) {
            assertThat(metaData.getColumnLabel(i + 1)).isEqualToIgnoringCase(labels[i]);
        }
        assertThat(metaData.getColumnCount()).isEqualTo(labels.length);
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public AssertResultSet isRecords(List<Object[]> target) throws SQLException {
        int count = 0;
        if (config.isCheckOrder()) {
            while (instance.next()) {
                assertThat(getRow()).isEqualTo(new Row(target.get(count)));
                ++count;
            }
        } else {
            while (instance.next()) {
                Row row = getRow();
                assertThat(row).isIn(target);
                ++count;
            }
        }
        assertThat(count).isEqualTo(target.size());
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public AssertResultSet rowCount(int rowCount) throws SQLException {
        int count = 0;
        while (instance.next()) {
            ++count;
        }
        assertThat(count).isEqualTo(rowCount);
        return this;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public class Row {
        private final Object[] tuple;

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
                Object actual = tuple[i];
                Object expected = values[i];
                if (actual instanceof Date) {
                    if (!actual.toString().equals(expected.toString())) {
                        return false;
                    }
                } else if (actual instanceof Time) {
                    if (config.getTimeDeviation() != 0) {
                        if (abs(((Time) actual).getTime() - ((Time) expected).getTime())
                            > config.getTimeDeviation()) {
                            return false;
                        }
                    } else if (!actual.toString().equals(values[i].toString())) {
                        return false;
                    }
                } else if (actual instanceof Timestamp) {
                    if (config.getTimestampDeviation() != 0) {
                        if (abs(((Timestamp) actual).getTime() - ((Timestamp) expected).getTime())
                            > config.getTimestampDeviation()) {
                            return false;
                        }
                    } else if (values[i] instanceof Long) {
                        if (((Timestamp) actual).getTime() / 1000 != (Long) values[i]) {
                            return false;
                        }
                    } else if (!actual.toString().equals(values[i].toString())) {
                        return false;
                    }
                } else if (!Objects.equals(actual, values[i])) {
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
