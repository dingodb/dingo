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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.common.util.CsvUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public final class AssertResultSet {
    private final ResultSet instance;

    private AssertResultSet(ResultSet obj) {
        instance = obj;
    }

    @Nonnull
    public static AssertResultSet of(ResultSet obj) {
        return new AssertResultSet(obj);
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

    public AssertResultSet isRecords(List<Object[]> target) throws SQLException {
        ResultSetMetaData metaData = instance.getMetaData();
        int size = metaData.getColumnCount();
        int count = 0;
        while (instance.next()) {
            Object[] row = new Object[size];
            for (int i = 0; i < size; ++i) {
                row[i] = instance.getObject(i + 1);
            }
            log.info("Get tuple {}.", row);
            assertThat(row).isIn(target);
            ++count;
        }
        assertThat(count).isEqualTo(target.size());
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public AssertResultSet isRecords(TupleSchema schema, String data) throws SQLException {
        try {
            List<Object[]> target = CsvUtils.readCsv(schema, data);
            return isRecords(target);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public AssertResultSet isRecordsInOrder(List<Object[]> target) throws SQLException {
        ResultSetMetaData metaData = instance.getMetaData();
        int size = metaData.getColumnCount();
        int count = 0;
        while (instance.next()) {
            Object[] row = new Object[size];
            for (int i = 0; i < size; ++i) {
                row[i] = instance.getObject(i + 1);
            }
            log.info("Get tuple {}.", row);
            assertThat(row).isEqualTo(target.get(count));
            ++count;
        }
        assertThat(count).isEqualTo(target.size());
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public AssertResultSet isRecordsInOrder(TupleSchema schema, String data) throws SQLException {
        try {
            List<Object[]> target = CsvUtils.readCsv(schema, data);
            return isRecordsInOrder(target);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
