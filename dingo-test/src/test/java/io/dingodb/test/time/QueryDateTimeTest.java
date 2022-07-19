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

package io.dingodb.test.time;

import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;

@Slf4j
public class QueryDateTimeTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile(QueryDateTimeTest.class.getResourceAsStream("table-test3-create.sql"));
        sqlHelper.execFile(QueryDateTimeTest.class.getResourceAsStream("table-test3-data.sql"));
    }

    @AfterAll
    public static void cleanUpAll() throws SQLException {
        sqlHelper.cleanUp();
    }

    @Test
    public void testFullScan() throws SQLException {
        String sql = "select date_column, timestamp_column, datetime_column from test3";
        sqlHelper.queryTest(
            sql,
            new String[]{"date_column", "timestamp_column", "datetime_column"},
            Collections.singletonList(new Object[]{"2003-12-31", 1447430881, "2007-1-31 23:59:59"})
        );
    }

    @Test
    void testDateFormatViaScan() throws SQLException {
        String sql = "select date_format(date_column, '%d %m %Y') as new_date_column from test3";
        sqlHelper.queryTest(
            sql,
            new String[]{"new_date_column"},
            Collections.singletonList(new Object[]{"31 12 2003"})
        );
    }

    @Test
    void testUnixTimeStampViaScan() throws SQLException {
        String sql = "select from_unixtime(timestamp_column) as new_datetime_column from test3";
        sqlHelper.queryTest(
            sql,
            new String[]{"new_datetime_column"},
            Collections.singletonList(new Object[]{new Timestamp(1447430881L * 1000L)})
        );
    }

    @Test
    void testDateDiffViaScan() throws SQLException {
        String sql = "select datediff(date_column, date_column) as new_diff from test3";
        sqlHelper.queryTest(
            sql,
            new String[]{"new_diff"},
            Collections.singletonList(new Object[]{0L})
        );
    }

    @Test
    void testDateTypeScan1() throws SQLException {
        String sql = "select date_type_column from test3";
        sqlHelper.queryTest(
            sql,
            new String[]{"date_type_column"},
            Collections.singletonList(new Object[]{"2003-12-31"})
        );
    }

    @Test
    void testDateTypeScan2() throws SQLException {
        String sql = "select time_type_column from test3";
        sqlHelper.queryTest(
            sql,
            new String[]{"time_type_column"},
            Collections.singletonList(new Object[]{"12:12:12"})
        );
    }
}
