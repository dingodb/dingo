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

import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.Services;
import io.dingodb.expr.runtime.op.time.utils.DateFormatUtil;
import io.dingodb.meta.test.MetaTestService;
import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DateTest {
    private static Connection connection;
    private static SqlHelper sqlHelper;
    // precision for minute
    private static final Long GLOBAL_TIME_PRECISION = 1000 * 60L;

    @BeforeAll
    public static void setupAll() throws Exception {
        connection = (sqlHelper = new SqlHelper()).getConnection();
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

    @Test
    public void testCurrentDate01() throws SQLException {
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
        LocalTime expected = LocalTime.now();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    Time time = rs.getTime(1);
                    System.out.println("Case1=> Time:" + time.toString());
                    // case1:
                    LocalTime real = time.toLocalTime();
                    Assertions.assertEquals(expected.getHour(), real.getHour());
                    Assertions.assertEquals(expected.getMinute(), real.getMinute());

                    // case2:
                    String timeStr = rs.getString(1);
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
                    LocalTime real2 = LocalTime.parse(timeStr, formatter);
                    System.out.println("Case2=> String:" + real2);
                    Assertions.assertEquals(expected.getHour(), real2.getHour());
                    Assertions.assertEquals(expected.getMinute(), real2.getMinute());

                    // case3:
                    String objectStr = rs.getObject(1).toString();
                    LocalTime real3 = LocalTime.parse(objectStr, formatter);
                    System.out.println("Case3=> Object :" + real3);
                    Assertions.assertEquals(expected.getHour(), real3.getHour());
                    Assertions.assertEquals(expected.getMinute(), real3.getMinute());
                }
            }
        }
    }

    @Test
    public void testCurTime() throws SQLException {
        String sql = "select curtime()";
        LocalTime expected = LocalTime.now();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    Time time = rs.getTime(1);
                    System.out.println("Case1=> Time:" + time.toString());
                    // case1:
                    LocalTime real = time.toLocalTime();
                    Assertions.assertEquals(expected.getHour(), real.getHour());
                    Assertions.assertEquals(expected.getMinute(), real.getMinute());

                    // case2:
                    String timeStr = rs.getString(1);
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
                    LocalTime real2 = LocalTime.parse(timeStr, formatter);
                    System.out.println("Case2=> String:" + real2);
                    Assertions.assertEquals(expected.getHour(), real2.getHour());
                    Assertions.assertEquals(expected.getMinute(), real2.getMinute());

                    // case3:
                    String objectStr = rs.getObject(1).toString();
                    LocalTime real3 = LocalTime.parse(objectStr, formatter);
                    System.out.println("Case3=> Object :" + real3);
                    Assertions.assertEquals(expected.getHour(), real3.getHour());
                    Assertions.assertEquals(expected.getMinute(), real3.getMinute());
                }
            }
        }
    }

    // Result like: 2022-03-30 16:49:57
    @Test
    public void testCurrentTimestamp() throws SQLException {
        String sql = "select current_timestamp";
        LocalDateTime localDateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String expectDateTime = localDateTime.format(formatter);

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    // case1:
                    Timestamp timestamp = rs.getTimestamp(1);
                    String result1 = formatter.format(timestamp.toLocalDateTime());
                    Assertions.assertEquals(expectDateTime, result1);
                    // case2:
                    System.out.println(rs.getObject(1));
                    Assertions.assertEquals(expectDateTime, rs.getObject(1).toString().substring(0, 19));
                    // case3:
                    Assertions.assertEquals(expectDateTime, rs.getString(1).substring(0, 19));
                }
            }
        }
    }

    @Test
    public void testCurrentTimestamp01() throws SQLException {
        String sql = "select current_timestamp()";
        LocalDateTime localDateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String expectDateTime = localDateTime.format(formatter);

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    // case1:
                    Timestamp timestamp = rs.getTimestamp(1);
                    String result1 = formatter.format(timestamp.toLocalDateTime());
                    Assertions.assertEquals(expectDateTime, result1);
                    // case2:
                    System.out.println(rs.getObject(1));
                    Assertions.assertEquals(expectDateTime, rs.getObject(1).toString().substring(0, 19));
                    // case3:
                    Assertions.assertEquals(expectDateTime, rs.getString(1).substring(0, 19));
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
                    assertThat(rs.getLong(1)).isEqualTo(1072800000);
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

    // YYYYMMDD hh:mm:ss Input
    // YYYYMMDD input
    @Test
    public void testDateFormatYYYYdMMdDDInput2() throws SQLException {
        String sql = "select date_format('1999-01-01 10:37:26', '%Y year %m month %d day and %s seconds')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1999 year 01 month 01 day and 26 seconds");
                }
            }
        }
    }

    @Test
    public void testDateFormatYYYYdMMdDDInput3() throws SQLException {
        String sql = "select date_format('1999-01-01 10:37:26', '%Y year %m month %d day and %s seconds')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1999 year 01 month 01 day and 26 seconds");
                }
            }
        }
    }

    // YYYY-MM-DD HH:mm:ss
    @Test
    public void testDateFormatYYYYdMMdDDeHHcmmcss1() throws SQLException {
        String sql = "select date_format('2022-04-13 10:37:36', '%H:%i:%S')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("10:37:36");
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
                    assertThat(rs.getString(1)).isEqualTo("-1");
                }
            }
        }
    }

    //select datediff('2022-04-13 15:17:58', '2022-05-31 00:01:01') as diffDate;
    @Test
    public void testDateDiff1() throws SQLException {
        String sql = "select datediff('2022-04-13 15:17:58', '2022-05-31 00:01:01') as diffDate";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("-48");
                }
            }
        }
    }

    @Test
    public void testDateDiff2() throws SQLException {
        String sql = "select datediff('2022-04-13 15:17:58', '2022-05-31 00:01:01') as diffDate";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("-48");
                }
            }
        }
    }

    @Test
    public void testDateDiff3() throws SQLException {
        String sql = "select datediff('2022-04-30 15:17:58', '2022-05-31 00:01:01') as diffDate";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("-31");
                }
            }
        }
    }

    // Result like: 1
    @Test
    public void testDateDiffOtherFormat() throws SQLException {
        String sql = "select datediff('2007-12-29','2007-12-30')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("-1");
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
            TupleSchema.ofTypes("STRING"),
            LocalDateTime.ofInstant(Instant.ofEpochMilli(1447430881000L), ZoneId.systemDefault())
                .format(DateFormatUtil.getDatetimeFormatter())
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

    // Check Cast function.
    @Test
    void testDateTypeInsert() throws SQLException {
        String createTableSQL = "create table timetest(id int, dt date,"
            + " primary key (id))";
        String insertSql = "insert into timetest values(11, '2022-11-01')";

        try (Statement statement = connection.createStatement()) {
            Boolean t = statement.execute(createTableSQL);
            System.out.println("testDateTypeInsert result: ");
            System.out.println(t);
            int count = statement.executeUpdate(insertSql);
            assertThat(count).isEqualTo(1);
        }
        String selectSql = "SELECT * from timetest";

        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    System.out.println(resultSet.getString(2));
                }
            }
        }
    }

    @Test
    void testDateTypeInsert1() throws SQLException {
        String createTableSQL = "create table timetest1(id int, dt date,"
            + " primary key (id))";
        String insertSql = "insert into timetest1 values(11, '2022-11-01')";

        try (Statement statement = connection.createStatement()) {
            Boolean t = statement.execute(createTableSQL);
            System.out.println("testDateTypeInsert result: ");
            System.out.println(t);
            int count = statement.executeUpdate(insertSql);
            assertThat(count).isEqualTo(1);
        }
        String selectSql = "SELECT * from timetest1";

        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    System.out.println(resultSet.getString(2));
                }
            }
        }
    }

    // Check Cast function.
    @Test
    void testTimestampTypeInsert() throws SQLException {
        String createTableSQL = "create table timetest3(id int, dt TIMESTAMP,"
            + " primary key (id))";
        String insertSql = "insert into timetest3 values(11, '2022-11-01 11:01:01')";

        try (Statement statement = connection.createStatement()) {
            Boolean t = statement.execute(createTableSQL);
            System.out.println("testDateTypeInsert result: ");
            System.out.println(t);
            int count = statement.executeUpdate(insertSql);
            assertThat(count).isEqualTo(1);
        }
        String selectSql = "SELECT * from timetest3";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    System.out.println(resultSet.getString(2));
                }
            }
        }
    }

    // Check Cast function.
    @Test
    void testCastDateTime() throws SQLException {
        String castSQL = "SELECT CAST('2020/11-01 01:01:01' AS TIMESTAMP)";

        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }

    @Test
    void testCastDateTime1() throws SQLException {
        String castSQL = "SELECT * from (SELECT * from (SELECT CAST('2020-11-01 01:01:01' AS TIMESTAMP)))";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }

    @Test
    void testCastDateTime2() throws SQLException {
        String castSQL = "SELECT * from (SELECT * from (SELECT CAST('2020-11-1 01:1:01' AS TIMESTAMP), "
            + "CAST('2020-1-1 01:1:1' AS TIMESTAMP)))";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }

    @Test
    void testCastDateTime3() throws SQLException {
        String castSQL = "SELECT CAST('2020/11-01 01:1:01' AS TIMESTAMP)";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }

    @Test
    void testCastWithDateTimeFormat() throws SQLException {
        String castSQL = "SELECT DATE_FORMAT(CAST('2020/11/30 01:1:01' AS TIMESTAMP), '%Y year, %m month')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }

    @Test
    void testCastWithDateTimeFormat1() throws SQLException {
        String castSQL = "SELECT DATE_FORMAT(CAST('2020.11.30 01:1:01' AS TIMESTAMP), '%Y year, %m month')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }

    @Test
    void testCastWithDateTimeFormat2() throws SQLException {
        String castSQL = "SELECT DATE_FORMAT(CAST('2020.11.30 9:1:1' AS TIMESTAMP), '%Y year, %m month')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }

    @Test
    void testCastWithDateFormat() throws SQLException {
        String castSQL = "SELECT DATE_FORMAT(CAST('2020.11.30' AS DATE), '%Y year, %m month')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }

    @Test
    void testCastWithDate1Format() throws SQLException {
        String castSQL = "SELECT DATE_FORMAT(CAST('2020/11/30' AS DATE), '%Y year, %m month')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }

    // Check Cast function.
    @Test
    void testTimeTypeInsert() throws SQLException {
        String createTableSQL = "create table timetest0(id int, create_time time,"
            + " primary key (id))";
        String insertSql = "insert into timetest0 values(11, '04:00:02')";

        try (Statement statement = connection.createStatement()) {
            Boolean t = statement.execute(createTableSQL);
            System.out.println("testDateTypeInsert result: ");
            System.out.println(t);
            int count = statement.executeUpdate(insertSql);
            assertThat(count).isEqualTo(1);
        }
        String selectSql = "SELECT * from timetest0";

        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    System.out.println(resultSet.getString(2));
                }
            }
        }
    }

    // Check Cast function.
    @Test
    void testTimeTypeInsert1() throws SQLException {
        String createTableSQL = "create table timetest2(id int, create_time time,"
            + " primary key (id))";
        String insertSql = "insert into timetest2 values(11, '04:30:02')";

        try (Statement statement = connection.createStatement()) {
            Boolean t = statement.execute(createTableSQL);
            System.out.println("testDateTypeInsert result: ");
            System.out.println(t);
            int count = statement.executeUpdate(insertSql);
            assertThat(count).isEqualTo(1);
        }
        String selectSql = "SELECT * from timetest2";

        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    System.out.println(resultSet.getString(2));
                }
            }
        }
    }

    @Test
    void testCastWithInsertTimeFormat() throws SQLException {
        String castSQL = "SELECT DATE_FORMAT(CAST('2020.11.30' AS DATE), '%Y year, %m month')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }
        }
    }
}
