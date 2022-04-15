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

package io.dingodb.test.datefuc;

import io.dingodb.calcite.Connections;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.Services;
import io.dingodb.expr.runtime.op.time.timeformatmap.DateFormatUtil;
import io.dingodb.meta.test.MetaTestService;
import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DateTest {
    private static Connection connection;
    private static SqlHelper sqlHelper;


    @BeforeAll
    public static void setupAll() throws Exception {
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).init(null);
        Services.initNetService();
        connection = Connections.getConnection(MetaTestService.SCHEMA_NAME);
        sqlHelper = new SqlHelper(connection);
        sqlHelper.execFile("/table-test3-create.sql");
        sqlHelper.execFile("/table-test3-data.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        connection.close();
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).clear();
    }

    //Result like: 2022-03-30 02:19:42
    @Test
    public void testNow() throws SQLException {
        String sql = "select now()";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
            }
        }
    }

    // Result like: 2022-03-30
    @Test
    public void testCurDate() throws SQLException {
        String sql = "select curdate()";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    String formatStr = DateFormatUtil.javaDefaultDateFormat();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(formatStr);
                    String dateTime = LocalDateTime.now().format(formatter);
                    System.out.println(dateTime);
                    assertThat(rs.getString(1)).isEqualTo(dateTime);
                }
            }
        }
    }

    // Result like: 2022-03-30
    @Test
    public void testCurTime() throws SQLException {
        String sql = "select curtime()";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    String formatStr = DateFormatUtil.javaDefaultDateFormat();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(formatStr);
                    String dateTime = new java.sql.Timestamp(System.currentTimeMillis())
                        .toLocalDateTime().format(formatter);
                    assertThat(rs.getString(1)).isEqualTo(dateTime);
                }
            }
        }
    }

    // Result like: 2022-03-30
    @Test
    public void testCurrentDate() throws SQLException {
        String sql = "select current_date";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    String formatStr = DateFormatUtil.javaDefaultDateFormat();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(formatStr);
                    String dateTime = LocalDateTime.now().format(formatter);
                    System.out.println(dateTime);
                    assertThat(rs.getString(1)).isEqualTo(dateTime);
                }
            }
        }
    }

    // Result like: 10:25:20
    @Test
    public void testCurrentTime() throws SQLException {
        String sql = "select current_time";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(new java.sql.Time(rs.getLong(1)));
                    assertThat(rs.getLong(1) / 1000)
                        .isEqualTo(System.currentTimeMillis() / 1000);
                }
            }
        }
    }

    // Result like: 2022-03-30 16:49:57
    @Test
    public void testCurrentTimestamp() throws SQLException {
        String sql = "select current_timestamp";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(new java.sql.Date(rs.getLong(1)) + " " + new java.sql.Time(rs.getLong(1)));
                    String formatStr = DateFormatUtil.defaultDatetimeFormat();
                    assertThat(rs.getLong(1) / 1000).isEqualTo(System.currentTimeMillis() / 1000);
                }
            }
        }
    }

    /*
     TODO : This test success timezone.
    Result like: 2015-11-13 16:08:01
    @Test
    public void testFromUnixTime() throws SQLException {
        String sql = "select from_unixtime(1447430881)";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    String formatStr = DateFormatUtil.defaultDatetimeFormat();
                    java.sql.Timestamp timestamp = new java.sql.Timestamp(1447430881L * 1000L);
                    assertThat(Timestamp.valueOf(rs.getString(1))).isEqualTo(timestamp);
                }
            }
        }
    }*/

    // Result like: 1072800000
    @Test
    public void testUnixTimeStamp() throws SQLException {
        String sql = "select unix_timestamp('2003-12-31')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    Date date = Date.valueOf("2003-12-31");
                    assertThat(rs.getLong(1)).isEqualTo(date.getTime() / 1000);
                }
            }
        }
    }

    //
    // YYYY-MM-DD Input
    @Test
    public void testDateFormatYYYYdMMdDD() throws SQLException {
        String sql = "select date_format('1999-01-01', '%Y-%m-%d')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1999-01-01");
                }
            }
        }
    }



    // YYYY/MM/DD Input
    @Test
    public void testDateFormatYYYYsMMsDDInput() throws SQLException {
        String sql = "select date_format('1999/01/01', '%Y/%m/%d')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1999/01/01");
                }
            }
        }
    }

    // YYYY-MM-DD HH:mm:ss
    @Test
    public void testDateFormatYYYYsMMsDDeHHcmmcssInput() throws SQLException {
        String sql = "select date_format('1999/01/01 01:01:01', '%Y/%m/%d %T')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1999/01/01 01:01:01");
                }
            }
        }
    }

    // YYYY.MM.DD
    @Test
    public void testDateFormatYYYYpMMpDDInput() throws SQLException {
        String sql = "select date_format('1999.01.01', '%Y.%m.%d')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1999.01.01");
                }
            }
        }
    }

    // YYYY.MM.DD HH:mm:ss
    @Test
    public void testDateFormatYYYYpMMpDDeHHcmmcssInput() throws SQLException {
        String sql = "select date_format('1999.01.01 01:01:01', '%Y.%m.%d %T')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1999.01.01 01:01:01");
                }
            }
        }
    }


    // YYYY-MM-DD Input
    @Test
    public void testDateFormatYYYYdMMdDDInput() throws SQLException {
        String sql = "select date_format('1999-01-01', '%Y/%m/%d')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1999/01/01");
                }
            }
        }
    }

    // YYYY-MM-DD HH:mm:ss
    @Test
    public void testDateFormatYYYYdMMdDDeHHcmmcss() throws SQLException {
        String sql = "select date_format('1999-01-01', '%Y-%m-%d %T')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1999-01-01 00:00:00");
                }
            }
        }
    }

    // YYYYMMDDHHmmss Input
    @Test
    public void testDateFormatYYYYmmDDHHmmssInput() throws SQLException {
        String sql = "select date_format('19990101010101', '%Y%m%d %T')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("19990101 01:01:01");
                }
            }
        }
    }

    // YYYYMMDD input
    @Test
    public void testDateFormatYYYYdMMdDDInput1() throws SQLException {
        String sql = "select date_format('1999-01-01', '%Y year %m month %d day')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1999 year 01 month 01 day");
                }
            }
        }
    }

    // Result like: 1
    @Test
    public void testDateDiff() throws SQLException {
        String sql = "select datediff('2007-12-29','2007-12-30')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1");
                }
            }
        }
    }

    // Result like: 1
    @Test
    public void testDateDiffOtherFormat() throws SQLException {
        String sql = "select datediff('20071229','2007-12-30')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1");
                }
            }
        }
    }

    @Test
    public void testFullScan() throws SQLException {
        String sql = "select date_column, timestamp_column, datetime_column from test3";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"date_column", "timestamp_column", "datetime_column"},
            TupleSchema.ofTypes("STRING", "INTEGER", "STRING"),
            "2003-12-31, 1447430881, 2007-1-31 23:59:59"
        );
    }


    @Test
    public void testOneColumnFullScan() throws SQLException {
        String sql = "select date_column as new_date_column from test3";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"new_date_column"},
            TupleSchema.ofTypes("STRING"),
            "2003-12-31"
        );
    }

    @Test
    void testDateFormatViaScan() throws SQLException {
        String sql = "select date_format(date_column, '%d %m %Y') as new_date_column from test3";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"new_date_column"},
            TupleSchema.ofTypes("STRING"),
            "31 12 2003"
        );
    }

    @Test
    void testUnixTimeStampViaScan() throws SQLException {
        String sql = "select from_unixtime(timestamp_column) as new_datetime_column from test3";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"new_datetime_column"},
            TupleSchema.ofTypes("TIMESTAMP"),
            "2015-11-13 16:08:01"
        );
    }

    @Test
    void testDateDiffViaScan() throws SQLException {
        String sql = "select datediff(date_column, date_column) as new_diff from test3";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"new_diff"},
            TupleSchema.ofTypes("LONG"),
            "0"
        );
    }

    @Test
    void testDateTypeScan() throws  SQLException {
        String sql = "select date_type_column, time_type_column, timestamp_type_column from test3";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"date_type_column", "time_type_column", "timestamp_type_column"},
            TupleSchema.ofTypes("DATE", "TIME", "TIMESTAMP"),
            "2003-12-31, 12:12:12, 1970-01-17 18:03:50"
        );

    }

    /* TODO: Support this test, when the corresponding rule added.
    @Test
    void testDateTypeInsert() throws SQLException{
        String insertSql = "INSERT INTO test3 values('2003-12-31', 1447430881, '2007-1-31 23:59:59', curdate()," +
            " curtime(), current_timestamp)";
        String insertSql = "INSERT INTO test3 values('2003-12-20', 1447430881, '2007-1-31 23:59:59', curDate(), " +
            "'12:12:12', 1447430881000)";
        try (Statement statement = connection.createStatement()) {
            int count = statement.executeUpdate(insertSql);
            assertThat(count).isEqualTo(1);
        }
        String selectSql = "SELECT * from test3";

        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while(resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }
     */


}
