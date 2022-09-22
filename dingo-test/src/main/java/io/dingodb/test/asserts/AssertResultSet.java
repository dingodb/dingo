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

import lombok.extern.slf4j.Slf4j;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public final class AssertResultSet {
    private final ResultSet instance;

    AssertResultSet(ResultSet obj) {
        instance = obj;
    }

    @Nonnull
    public static Object[] getRow(@Nonnull ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int size = metaData.getColumnCount();
        Object[] row = new Object[size];
        for (int i = 0; i < size; ++i) {
            int type = metaData.getColumnType(i + 1);
            if (type == Types.DATE || type == Types.TIME) {
                // Compare Date & Time type by string.
                // NOTE: Milliseconds are lost.
                row[i] = resultSet.getString(i + 1);
            } else {
                row[i] = resultSet.getObject(i + 1);
            }
        }
        return row;
    }

    @SuppressWarnings("UnusedReturnValue")
    public AssertResultSet isPlan(String... name) throws SQLException {
        assertThat(instance.getMetaData().getColumnCount()).isEqualTo(1);
        assertThat(instance.getMetaData().getColumnName(1)).isEqualTo("PLAN");
        int rowCount = 0;
        while (instance.next()) {
            List<String> plan = Arrays.stream(instance.getString(1).split("\n"))
                .map(String::trim)
                .collect(Collectors.toList());
            assertThat(plan.size()).isEqualTo(name.length);
            for (int i = 0; i < name.length; ++i) {
                assertThat(plan.get(i)).startsWith(name[i]);
            }
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
        while (instance.next()) {
            assertThat(getRow(instance)).isIn(target);
            ++count;
        }
        assertThat(count).isEqualTo(target.size());
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public AssertResultSet isRecordsInOrder(List<Object[]> target) throws SQLException {
        int count = 0;
        while (instance.next()) {
            assertThat(getRow(instance)).isEqualTo(target.get(count));
            ++count;
        }
        assertThat(count).isEqualTo(target.size());
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public AssertResultSet isRecordsInOrderWithApproxTime(List<Object[]> target) throws SQLException {
        ResultSetMetaData metaData = instance.getMetaData();
        int size = metaData.getColumnCount();
        int count = 0;
        while (instance.next()) {
            Object[] expectedRow = target.get(count);
            for (int i = 0; i < size; ++i) {
                Object value = instance.getObject(i + 1);
                Object expected = expectedRow[i];
                if (value instanceof Date) {
                    assertThat(value.toString()).isEqualTo(expected.toString());
                } else if (value instanceof Time) {
                    if (expected instanceof String) {
                        assertThat(value.toString()).isEqualTo(expected);
                    } else {
                        assertThat((Time) value).isCloseTo((Time) expected, 5L * 1000L);
                    }
                } else if (value instanceof Timestamp) {
                    assertThat((Timestamp) value)
                        .isCloseTo((Timestamp) expected, 5L * 1000L);
                } else {
                    assertThat(value).isEqualTo(expected);
                }
            }
            count++;
        }
        assertThat(count).isEqualTo(target.size());
        return this;
    }
}
