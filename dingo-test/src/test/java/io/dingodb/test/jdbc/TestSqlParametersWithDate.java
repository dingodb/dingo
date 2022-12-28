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

package io.dingodb.test.jdbc;

import com.google.common.collect.ImmutableList;
import io.dingodb.test.SqlHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSqlParametersWithDate {
    protected static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @Test
    public void testInsert() throws SQLException {
        SqlHelper.RandomTable table = sqlHelper.randomTable().execSqls("create table {table} ("
            + "id int,"
            + "data date,"
            + "data1 time,"
            + "data2 timestamp,"
            + "primary key(id)"
            + ")"
        );
        String sql = "insert into " + table + " values(?, ?, ?, ?)";
        Connection connection = sqlHelper.getConnection();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 1);
            statement.setDate(2, new Date(0));
            statement.setTime(3, new Time(0));
            statement.setTimestamp(4, new Timestamp(0));
            int count = statement.executeUpdate();
            assertThat(count).isEqualTo(1);
            statement.setInt(1, 2);
            statement.setDate(2, new Date(86400000));
            statement.setTime(3, new Time(3600000));
            statement.setTimestamp(4, new Timestamp(1));
            count = statement.executeUpdate();
            assertThat(count).isEqualTo(1);
        }
        sqlHelper.queryTest(
            "select * from " + table,
            new String[]{"id", "data", "data1", "data2"},
            ImmutableList.of(
                new Object[]{1, new Date(0).toString(), new Time(0).toString(), new Timestamp(0)},
                new Object[]{2, new Date(86400000).toString(), new Time(3600000).toString(), new Timestamp(1)}
            )
        );
        table.drop();
    }
}
