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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@Disabled("Failed to parse to Time.")
public class DateTest {

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static Connection connection;
    private static SqlHelper sqlHelper;
    // precision for minute
    private static final Long GLOBAL_TIME_PRECISION = 1000 * 60L;

    private static final Duration ERROR_RANGE = Duration.ofSeconds(1);

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

    public void assertTemporalWithErrorRange(Temporal time1, Temporal time2) {
        assertThat(Duration.between(time1, time2)).isLessThan(ERROR_RANGE);
    }

    //Result like: 2022-03-30 02:19:42
    @Test
    public void testNow() throws SQLException {
        String sql = "select now()";
        LocalDateTime expected = LocalDateTime.now();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    // case1:
                    assertTemporalWithErrorRange(expected, rs.getTimestamp(1).toLocalDateTime());
                    // case2:
                    assertTemporalWithErrorRange(
                        expected,
                        LocalDateTime.parse(rs.getObject(1).toString().substring(0, 19), DATETIME_FORMATTER)
                    );
                    // case3:
                    assertTemporalWithErrorRange(expected, LocalDateTime.parse(rs.getString(1), DATETIME_FORMATTER));
                }
            }
        }
    }

    @Test
    public void testCurDate() throws SQLException {
        String sql = "select curdate()";
        LocalDate expected = LocalDate.now();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    assertThat(LocalDate.parse(rs.getString(1), DATE_FORMATTER)).isEqualTo(expected);
                }
            }
        }
    }

    // Result like: 2022-03-30
    @Test
    public void testCurrentDate() throws SQLException {
        String sql = "select current_date";
        LocalDate expected = LocalDate.now();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    assertThat(LocalDate.parse(rs.getString(1), DATE_FORMATTER)).isEqualTo(expected);
                }
            }
        }
    }

    // Result like: 2022-03-30
    @Test
    public void testCurrentDate01() throws SQLException {
        String sql = "select current_date";
        LocalDate expected = LocalDate.now();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    assertThat(LocalDate.parse(rs.getString(1), DATE_FORMATTER)).isEqualTo(expected);
                }
            }
        }
    }

    @Test
    public void testMultiConcatFunction() throws SQLException {
        String sql = "select current_date() || ' ' || current_time()";
        LocalDateTime expected = LocalDateTime.now();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    assertTemporalWithErrorRange(expected, LocalDateTime.parse(rs.getString(1), DATETIME_FORMATTER));
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
                while (rs.next()) {
                    // case1:
                    assertTemporalWithErrorRange(expected, rs.getTime(1).toLocalTime());

                    // case2:
                    assertTemporalWithErrorRange(expected, LocalTime.parse(rs.getString(1), TIME_FORMATTER));

                    // case3:
                    assertTemporalWithErrorRange(
                        expected,
                        LocalTime.parse(rs.getObject(1).toString(), TIME_FORMATTER)
                    );
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
                while (rs.next()) {
                    // case1:
                    assertTemporalWithErrorRange(expected, rs.getTime(1).toLocalTime());

                    // case2:
                    assertTemporalWithErrorRange(expected, LocalTime.parse(rs.getString(1), TIME_FORMATTER));

                    // case3:
                    assertTemporalWithErrorRange(
                        expected,
                        LocalTime.parse(rs.getObject(1).toString(), TIME_FORMATTER)
                    );
                }
            }
        }
    }

    @Test
    public void testCurTimeWithConcat() throws SQLException {
        String prefix = "test-";
        String sql = "select '" + prefix + "' || curtime()";
        LocalTime expected = LocalTime.now();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    assertThat(rs.getString(1)).startsWith(prefix);
                    assertTemporalWithErrorRange(
                        expected,
                        LocalTime.parse(rs.getString(1).substring(prefix.length()), TIME_FORMATTER)
                    );
                }
            }
        }
    }

    // Result like: 2022-03-30 16:49:57
    @Test
    public void testCurrentTimestamp() throws SQLException {
        String sql = "select current_timestamp";
        LocalDateTime expected = LocalDateTime.now();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    // case1:
                    assertTemporalWithErrorRange(expected, rs.getTimestamp(1).toLocalDateTime());
                    // case2:
                    assertTemporalWithErrorRange(
                        expected,
                        LocalDateTime.parse(rs.getObject(1).toString().substring(0, 19), DATETIME_FORMATTER)
                    );
                    // case3:
                    assertTemporalWithErrorRange(expected, LocalDateTime.parse(rs.getString(1), DATETIME_FORMATTER));
                }
            }
        }
    }

    @Test
    public void testCurrentTimestamp01() throws SQLException {
        String sql = "select current_timestamp()";
        LocalDateTime expected = LocalDateTime.now();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    // case1:
                    assertTemporalWithErrorRange(expected, rs.getTimestamp(1).toLocalDateTime());
                    // case2:
                    assertTemporalWithErrorRange(
                        expected,
                        LocalDateTime.parse(rs.getObject(1).toString().substring(0, 19), DATETIME_FORMATTER)
                    );
                    // case3:
                    assertTemporalWithErrorRange(expected, LocalDateTime.parse(rs.getString(1), DATETIME_FORMATTER));
                }
            }
        }
    }

    @Test
    public void testCurrentTimestamp02() throws SQLException {
        final String prefix = "test-";
        String sql = "select '" + prefix + "' || current_timestamp()";
        LocalDateTime expected = LocalDateTime.now();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    assertThat(rs.getString(1)).startsWith(prefix);
                    assertTemporalWithErrorRange(
                        expected,
                        LocalDateTime.parse(rs.getString(1).substring(prefix.length()), DATETIME_FORMATTER)
                    );
                }
            }
        }
    }

    @Test
    public void testUnixTimeStamp01() throws SQLException {
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


    @Test
    public void testUnixTimeStamp02() throws SQLException {
        String sql = "select unix_timestamp('2022/4/21')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getLong(1)).isEqualTo(1650470400);
                }
            }
        }
    }

    @Test
    public void testUnixTimeStamp03() throws SQLException {
        String sql = "select unix_timestamp(current_date())";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                LocalDate date = LocalDate.now();
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getLong(1)).isEqualTo(date.atStartOfDay().toInstant(
                        ZoneOffset.ofHours(8)).toEpochMilli() / 1000);
                }
            }
        }
    }

    @Test
    public void testUnixTimeStamp04() throws SQLException {
        String sql = "select unix_timestamp(curdate())";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                LocalDate date = LocalDate.now();
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getLong(1)).isEqualTo(date.atStartOfDay().toInstant(
                        ZoneOffset.ofHours(8)).toEpochMilli() / 1000);
                }
            }
        }
    }

    @Test
    public void testUnixTimeStamp05() throws SQLException {
        String sql = "select unix_timestamp(curdate())";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                LocalDate date = LocalDate.now();
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getLong(1)).isEqualTo(date.atStartOfDay().toInstant(
                        ZoneOffset.ofHours(8)).toEpochMilli() / 1000);
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
        String sql = "select date_format('1999/1/01 01:01:01', '%Y/%m/%d %T')";
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
        String sql = "select datediff('2022-04-14 15:17:58', '2022-05-31 00:01:01') as diffDate";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("-47");
                }
            }
        }
    }

    @Test
    public void testDateDiff2() throws SQLException {
        String sql = "select datediff('2022-04-14 15:17:58', '2022-05-31 00:01:01') as diffDate";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("-47");
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

    // bad case
    @Test
    @Disabled
    public void testDateDiff4() throws SQLException {
        String sql = "select datediff('2022-04-30', '2022-05-01') as diffDate";
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

    // Result like: -30
    @Test
    public void testDateDiffOtherFormat1() throws SQLException {
        String sql = "select datediff('2022-5-1', '2022-05-31') as diffdate";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("-30");
                }
            }
        }
    }

    // Result like: -30
    @Test
    public void testDateDiffOtherFormat2() throws SQLException {
        String sql = "select datediff('2022-12-31',Current_Timestamp()) as diffDate";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    Long delta = LocalDate.of(2022, 12, 31).toEpochDay() - LocalDate.now().toEpochDay();
                    assertThat(rs.getString(1)).isEqualTo(String.valueOf(delta.intValue()));
                }
            }
        }
    }

    @Test
    public void testDateDiffOtherFormat3() throws SQLException {
        String sql = "select datediff('2022-12-31',now()) as diffDate";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    Long delta = LocalDate.of(2022, 12, 31).toEpochDay() - LocalDate.now().toEpochDay();
                    assertThat(rs.getString(1)).isEqualTo(String.valueOf(delta.intValue()));
                }
            }
        }
    }

    @Test
    public void testDateDiffOtherFormat4() throws SQLException {
        String sql = "select datediff('2022-12-31',Current_Timestamp) as diffDate";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    Long delta = LocalDate.of(2022, 12, 31).toEpochDay() - LocalDate.now().toEpochDay();
                    assertThat(rs.getString(1)).isEqualTo(String.valueOf(delta.intValue()));
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
                    assertThat(resultSet.getString(2)).isEqualTo("2022-11-01");
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
                    assertThat(resultSet.getString(2)).isEqualTo("2022-11-01");
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
                    assertThat(resultSet.getString(2)).isEqualTo("2022-11-01 11:01:01");
                }
            }
        }
    }

    // Check Cast function. not support.
    @Test
    @Disabled
    void testCastDateTime() throws SQLException {
        String castSQL = "SELECT CAST('2020/11-01 01:01:01' AS TIMESTAMP)";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    assertThat(resultSet.getString(1)).isEqualTo("2020-11-01 01:01:01");
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
                    assertThat(resultSet.getString(1)).isEqualTo("2020-11-01 01:01:01");
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
                    assertThat(resultSet.getString(1)).isEqualTo("2020-11-01 01:01:01");
                }
            }
        }
    }

    // This case is not supported.
    @Test
    @Disabled
    void testCastDateTime3() throws SQLException {
        String castSQL = "SELECT CAST('2020/11-01 01:1:01' AS TIMESTAMP)";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    assertThat(resultSet.getString(1)).isEqualTo("2020 year, 11 month");
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
                    assertThat(resultSet.getString(1)).isEqualTo("2020 year, 11 month");
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
                    assertThat(resultSet.getString(1)).isEqualTo("2020 year, 11 month");
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
                    assertThat(resultSet.getString(1)).isEqualTo("2020 year, 11 month");
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
                    assertThat(resultSet.getString(1)).isEqualTo("2020 year, 11 month");
                }
            }
        }
    }


    @Test
    void testCastWithDate1Format() throws SQLException {
        String castSQL = "SELECT DATE_FORMAT(CAST('2020/11/3' AS DATE), '%Y year, %m month %d day')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    assertThat(resultSet.getString(1)).isEqualTo("2020 year, 11 month 03 day");
                }
            }
        }
    }

    @Test
    void testCastWithDate1Format2() throws SQLException {
        String castSQL = "select date_format('2022-04-1', '%Y-%m-%d')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    assertThat(resultSet.getString(1)).isEqualTo("2022-04-01");
                }
            }
        }
    }

    @Test
    void testCastWithDate1Format3() throws SQLException {
        String castSQL = "select date_format('2022-04-13 10:37:26', '%m month and %d day of Year %Y, %H hour %i "
            + "minutes and %S seconds')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    assertThat(resultSet.getString(1)).isEqualTo("04 month and 13 day of Year 2022, "
                        + "10 hour 37 minutes and 26 seconds");
                }
            }
        }
    }

    @Test
    void testCastWithDate1Format4() throws SQLException {
        String castSQL = "select date_format('2022-04-13 10:37:26', '%Ss')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    assertThat(resultSet.getString(1)).isEqualTo("26s");
                }
            }
        }
    }

    // Check Cast function. checked.
    @Test
    @Disabled
    void testTimeTypeInsert() throws SQLException {
        String createTableSQL = "create table timetest0(id int, create_time time,"
            + " primary key (id))";
        String insertSql = "insert into timetest0 values(11, '04:70:02')";

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
    @Disabled
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
                    System.out.println(resultSet.getString(2));
                    assertThat(resultSet.getString(2)).isEqualTo("04:30:02");
                }
            }
        }
    }

    @Test
    void testCastTimeFormat() throws SQLException {
        String castSQL = "SELECT CAST('1970.1.2' AS DATE)";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    assertThat(resultSet.getString(1)).isEqualTo("1970-01-02");
                }
            }
        }
    }

    @Test
    void testCastWithTimeFormat1() throws SQLException {
        String castSQL = "SELECT DATE_FORMAT(CAST('2020.11.30' AS DATE), '%Y year, %m month')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet =  statement.executeQuery(castSQL)) {
                System.out.println("Result: ");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                    assertThat(resultSet.getString(1)).isEqualTo("2020 year, 11 month");
                }
            }
        }
    }


    @Test
    @Disabled
    void testJavaLocalDateType() throws SQLException {
        // case 1, good case. single M can parse all the valid month.
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("y-M-d");
        LocalDate ldt = LocalDate.parse("1909-12-01", dtf);
        System.out.println(ldt);

        // case 2, good case. single pattern parse all the valid month.
        LocalDate ldt1 = LocalDate.parse("1909-1-01", dtf);
        System.out.println(ldt1);

        // case 3, good case. single pattern parse all the valid month.
        DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("y.M.d");
        LocalDate ldt2 = LocalDate.parse("1909.1.01", dtf2);
        System.out.println(ldt2);

        // Text '18-13-01' could not be parsed: Invalid value for MonthOfYear (valid values 1 - 12): 13
        // case 4, bad case, field in datetime out of range.
        LocalDate ldt3 = LocalDate.parse("18-12-01", dtf);
        System.out.println(ldt3);

    }

    @Test
    @Disabled
    void testJavaLocalDateTimeType() throws SQLException {
        // case 1, good case. single M can parse all the valid month.
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("y-M-d H:m:s");
        LocalDateTime ldt = LocalDateTime.parse("1909-14-01 00:00:00", dtf);
        System.out.println(ldt);
        // case 2, good case. single m can parse all the valid minute
        LocalDateTime ldt3 = LocalDateTime.parse("1909-12-01 00:44:06", dtf);
        System.out.println(ldt3);

        // case 3, good case. single s can parse all the valid second
        LocalDateTime ldt4 = LocalDateTime.parse("1909-12-01 00:10:06", dtf);
        System.out.println(ldt4);

        // java.time.format.DateTimeParseException: Text '1909-12-44 01:10:06'
        // could not be parsed: Invalid value for DayOfMonth (valid values 1 - 28/31): 44
        // case 4, bad case. Some field of datetime out of range.
        LocalDateTime ldt5 = LocalDateTime.parse("1909-12-4 01:10:06", dtf);
        System.out.println(ldt5);


    }

    @Test
    @Disabled
    void testJavaTimeLocalTime2TimeType() throws SQLException {
        // case 1, good case. single M can parse all the valid month.
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("H:m:s");
        LocalTime lt = LocalTime.parse("19:01:01", dtf);
        System.out.println(lt);
        Time t = Time.valueOf(lt);
        System.out.println(t);
    }

    @Test
    @Disabled
    void testJavaTimeLocalDate2DateType() throws SQLException {
        // case 1, good case. single M can parse all the valid month.
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("y-M-d");
        LocalDate ld = LocalDate.parse("1909-12-01", dtf);
        System.out.println(ld);
        Date d = Date.valueOf(ld);
        System.out.println(d);
    }

    @Test
    @Disabled
    void testJavaTimeLocalDateTime2DateTimeType() throws SQLException {
        // case 1, good case. single M can parse all the valid month.
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("y-M-d H:m:s");
        System.out.println("dtf: " + dtf);
        LocalDateTime ldt = LocalDateTime.parse("1909-12-01 00:00:00", dtf);
        System.out.println(ldt);
        Timestamp ts = Timestamp.valueOf(ldt);
        System.out.println(ts);
    }

    @Test
    @Disabled
    void testCustomizedDateTimeFormat() throws SQLException {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("y-M-d H:m:s");
        LocalDateTime ldt = LocalDateTime.parse("1909-1-01 00:00:00", dtf);
        System.out.println(String.format("%tm month", ldt.getMonthValue()));
        System.out.println(String.format("%td day", ldt.getDayOfMonth()));
        System.out.println(String.format("%ty year", ldt.getYear()));
    }

    @Test
    @Disabled
    void testLocalDateTimeCompareWithDateString() throws SQLException {
        String dt = "2020/04/31 00:00:00";
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("y/M/d H:m:s");
        LocalDateTime ldt = LocalDateTime.parse(dt, dtf);
        String[] dts = dt.split(" ")[0].split("/");
        System.out.println("Result: ");
        for (String d: dts) {
            System.out.println(d);
        }
        System.out.println(dts);
        System.out.println("end");
    }

    @Test
    @Disabled
    void testyyyymmddhhmmssDate() throws SQLException {
        String d = "20200101112233";
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime dt = LocalDateTime.parse(d, dtf);
        System.out.println(dt);
    }

    @Test
    @Disabled
    void testyyyymmddDate() throws SQLException {
        String d = "20200101";
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDate ld = LocalDate.parse(d, df);
        System.out.println(ld);
    }

    @Test
    @Disabled
    void testMatcher() throws SQLException {
        List<Pattern> timeRexPatList = Stream.of(
            Pattern.compile("[0-9]{8}([0-9]{6}){0,1}"),
            Pattern.compile("[0-9]+-[0-9]+-[0-9]+(\\ [0-9]+:[0-9]+:[0-9]+){0,1}"),
            Pattern.compile("[0-9]+/[0-9]+/[0-9]+(\\ [0-9]+:[0-9]+:[0-9]+){0,1}"),
            Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+(\\ [0-9]+:[0-9]+:[0-9]+){0,1}")
        ).collect(Collectors.toList());
        System.out.println(timeRexPatList.get(0));
        String dt = "20010101";
        if (timeRexPatList.get(0).matcher(dt).matches()) {
            System.out.println(dt + " matches");
        }
        dt = "20200101112233";
        if (timeRexPatList.get(0).matcher(dt).matches()) {
            System.out.println(dt + " matches");
        }
        dt = "2020.01.01";
        if (timeRexPatList.get(3).matcher(dt).matches()) {
            System.out.println(dt + " matches");
        }
        dt = "2020.01.01 11:11:11";
        if (timeRexPatList.get(3).matcher(dt).matches()) {
            System.out.println(dt + " matches");
        }
        List<DateTimeFormatter> datetimes = Stream.of(
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss"),
            DateTimeFormatter.ofPattern("y/M/d H:m:s"),
            DateTimeFormatter.ofPattern("y.M.d H:m:s"),
            DateTimeFormatter.ofPattern("y-M-d H:m:s")
        ).collect(Collectors.toList());
        System.out.println(datetimes.get(0));
    }

    @Test
    @Disabled
    void testTimePart() throws SQLException {
        Pattern timePartPattern = Pattern.compile("([0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})");
        String dt0 = "16:52:17.1756, 16:54:17.1756";

        System.out.println("Result: ");
        Matcher m = timePartPattern.matcher(dt0);
        if (m.find()) {
            System.out.println(m.group());
        }
        while (m.find()) {
            System.out.println(m.group());
        }

        String dt1 = "2022-04-27 16:53:17.175";
        System.out.println("Result :");
        m = timePartPattern.matcher(dt1);
        while (m.find()) {
            System.out.println(m.group());
        }

        String dt2 = "2022-04-27 16:54:17.17";
        System.out.println("Result :");
        m = timePartPattern.matcher(dt2);
        while (m.find()) {
            System.out.println(m.group());
        }
    }
}
