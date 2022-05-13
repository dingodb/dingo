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

import io.dingodb.exec.Services;
import io.dingodb.expr.runtime.op.time.utils.DingoDateTimeUtils;
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

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class OnlySimpleDateFunctionTest {

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static Connection connection;
    private static SqlHelper sqlHelper;
    // precision for minute
    private static final Long GLOBAL_TIME_PRECISION = 1000 * 60L;

    private static final Duration ERROR_RANGE = Duration.ofSeconds(2);

    @BeforeAll
    public static void setupAll() throws Exception {
        connection = (sqlHelper = new SqlHelper()).getConnection();
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
                System.out.println("Result: ");

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
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(LocalDate.parse(rs.getString(1), DATE_FORMATTER)).isEqualTo(expected);
                }
            }
        }
    }

    @Test
    public void testCurDate1() throws SQLException {
        String sql = "select curdate()";
        LocalDate expected = LocalDate.now();

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
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
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
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
                System.out.println("Result: ");

                while (rs.next()) {
                    System.out.println(rs.getString(1));
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
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
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
                    System.out.println("Result: ");
                    System.out.println(rs.getString(1));
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
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
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
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
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
        String sql = "select unix_timestamp('2022-04-14')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    LocalDate localDate = DingoDateTimeUtils.convertToDate("2022-04-14");
                    Date d =  new Date(localDate.atStartOfDay().toInstant(DingoDateTimeUtils.getLocalZoneOffset())
                        .toEpochMilli());
                    String targetString = String.valueOf((d.getTime() / 1000));
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo(targetString);
                }
            }
        }
    }


    @Test
    public void testUnixTimeStamp02() throws SQLException {
        String sql = "select unix_timestamp('2022/4/14')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    LocalDate localDate = DingoDateTimeUtils.convertToDate("2022-04-14");
                    Date d =  new Date(localDate.atStartOfDay().toInstant(DingoDateTimeUtils.getLocalZoneOffset())
                        .toEpochMilli());
                    Long target = (d.getTime() / 1000);
                    System.out.println(rs.getString(1));
                    assertThat(rs.getLong(1)).isEqualTo(target);
                }
            }
        }
    }

    @Test
    public void testUnixTimeStamp03() throws SQLException {
        String sql = "select unix_timestamp(current_date())";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    Date date = new Date(LocalDate.now().atStartOfDay().toInstant(DingoDateTimeUtils
                        .getLocalZoneOffset()).toEpochMilli());
                    System.out.println(rs.getString(1));
                    assertThat(rs.getLong(1)).isEqualTo(date.getTime() / 1000);
                }
            }
        }
    }


    @Test
    public void testUnixTimeStamp04() throws SQLException {
        String sql = "select unix_timestamp(curdate())";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    Date date = new Date(LocalDate.now().atStartOfDay().toInstant(DingoDateTimeUtils
                        .getLocalZoneOffset()).toEpochMilli());
                    System.out.println(rs.getString(1));
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

    @Test
    public void testDateFormatYYYYdMMdDDInput4() throws SQLException {
        String sql = "select date_format('2022-04-13 10:37:26', '%m month and %d day of "
            + "Year %Y, %H hour %i minutes and %S seconds')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("04 month and 13 day of Year 2022, "
                        + "10 hour 37 minutes and 26 seconds");
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
    public void testLocalTimeParse() throws SQLException {
        String timeStr = "111213";
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HHmmss");
        LocalTime localTime = LocalTime.parse(timeStr, dtf);
        System.out.println(localTime);
    }

    @Test
    public void testAddDate() {
        Date d = Date.valueOf("2020-01-01");
        LocalDate ld = LocalDate.now();
        LocalDate tomorrow = ld.plusDays(1);
        System.out.println(tomorrow);
    }

    @Test
    public void testTimeFormat() throws SQLException {
        String sql = "select time_format('111213', '%H-%i.%s')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("11-12.13");
                }
            }
        }
    }

    @Test
    public void testTimeFormat1() throws SQLException {
        String sql = "select time_format('11:2:3', '%H.%i.%s')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("11.02.03");
                }
            }
        }
    }

    @Test
    public void testGetZoneOffset() {
        Instant instant = Instant.now(); //can be LocalDateTime
        ZoneId systemZone = ZoneId.systemDefault(); // my timezone
        System.out.println(systemZone);
        ZoneOffset currentOffsetForMyZone = systemZone.getRules().getOffset(instant);
        System.out.println("Result: ");
        System.out.println(currentOffsetForMyZone);
    }

    @Test
    public void testDateToTimestamp() throws SQLException {
        LocalDate localDate = DingoDateTimeUtils.convertToDate("2022-04-14");
        Date d =  new Date(localDate.atStartOfDay().toInstant(DingoDateTimeUtils.getLocalZoneOffset()).toEpochMilli());
        System.out.println(d.getTime());
    }

    @Test
    public void testDateTimetoTimestamp() throws SQLException {
        LocalDateTime localDateTime = DingoDateTimeUtils.convertToDatetime("20220414180215");
        Timestamp ts = new Timestamp(localDateTime.toEpochSecond(DingoDateTimeUtils.getLocalZoneOffset()) * 1000);
        System.out.println(ts.getTime());
    }
}
