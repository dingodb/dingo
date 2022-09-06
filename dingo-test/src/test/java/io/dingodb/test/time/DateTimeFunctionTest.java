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
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Slf4j
public class DateTimeFunctionTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @Nonnull
    private static Stream<Arguments> getArguments() {
        return Stream.of(
            // date_format
            arguments("select date_format('1999-01-01', '%Y-%m-%d')", "1999-01-01"),
            arguments("select date_format('1999/01/01', '%Y/%m/%d')", "1999/01/01"),
            arguments("select date_format('1999.01.01', '%Y.%m.%d')", "1999.01.01"),
            arguments("select date_format('1999-01-01', '%Y-%m-%d %T')", "1999-01-01 00:00:00"),
            arguments("select date_format('1999-01-01', '%Y year %m month %d day')", "1999 year 01 month 01 day"),
            arguments("select date_format('2022-04-1', '%Y-%m-%d')", "2022-04-01"),
            arguments("select date_format('2022-04-1', '%Y-%m-%d %A')", "2022-04-01 A"),
            arguments("select date_format('20220413', 'Year:%Y Month:%m Day:%d')", "Year:2022 Month:04 Day:13"),
            // time_format
            arguments("select time_format('111213', '%H-%i.%s')", "11-12.13"),
            arguments("select time_format('11:2:3', '%H.%i.%s')", "11.02.03"),
            arguments("select time_format('110203', '%H%i%S')", "110203"),
            arguments("select time_format('083026', '%H.%i.%s')", "08.30.26"),
            arguments("select time_format('000006', '%H.%i.%s')", "00.00.06"),
            arguments("select time_format('180000', '%H.%i.%s')", "18.00.00"),
            arguments("select time_format('23:59:59', '%H.%i.%s')", "23.59.59"),
            arguments("select time_format('19:00:28.331', '%H.%i.%s.%f')", "19.00.28.331"),
            // timestamp_format
            arguments("select timestamp_format('1999/1/01 01:01:01', '%Y/%m/%d %T')", "1999/01/01 01:01:01"),
            arguments("select timestamp_format('1999.01.01 01:01:01', '%Y.%m.%d %T')", "1999.01.01 01:01:01"),
            arguments("select timestamp_format('19990101010101', '%Y%m%d %T')", "19990101 01:01:01"),
            arguments("select timestamp_format('20220413103706','%Y/%m/%d %H:%i:%S')", "2022/04/13 10:37:06"),
            arguments("select timestamp_format('1999-01-01 01:01:01.22', '%Y/%m/%d %T.%f')", "1999/01/01 01:01:01.220"),
            arguments("select timestamp_format('2022-04-13 10:37:26', '%Ss')", "26s"),
            arguments(
                "select timestamp_format('1999-01-01 10:37:26', '%Y year %m month %d day and %s seconds')",
                "1999 year 01 month 01 day and 26 seconds"
            ),
            arguments(
                "select timestamp_format('2022-04-13 10:37:26', '%m mon %d day %Y year, %H hour %i min %S sec')",
                "04 mon 13 day 2022 year, 10 hour 37 min 26 sec"
            ),
            arguments("select timestamp_format('2022-04-13 10:37:36', '%H:%i:%S')", "10:37:36"),
            arguments(
                "select timestamp_format('20220413103726', 'Year:%Y Month:%m Day:%d')",
                "Year:2022 Month:04 Day:13"
            ),
            // date_format with cast
            arguments(
                "SELECT DATE_FORMAT(CAST('2020/11/3' AS DATE), '%Y year, %m month %d day')",
                "2020 year, 11 month 03 day"
            ),
            arguments(
                "SELECT DATE_FORMAT(CAST('2020.11.30' AS DATE), '%Y year, %m month')",
                "2020 year, 11 month"
            ),
            // timestamp_format w/ cast
            arguments(
                "SELECT TIMESTAMP_FORMAT(CAST('2020.11.30 9:1:1' AS TIMESTAMP), '%Y year, %m month')",
                "2020 year, 11 month"
            ),
            arguments(
                "SELECT TIMESTAMP_FORMAT(CAST('2020.11.30 01:1:01' AS TIMESTAMP), '%Y year, %m month')",
                "2020 year, 11 month"
            ),
            arguments(
                "SELECT TIMESTAMP_FORMAT(CAST('2020/11/30 01:1:01' AS TIMESTAMP), '%Y year, %m month')",
                "2020 year, 11 month"
            ),
            // cast
            arguments("SELECT CAST('2020-11-01 01:01:01' AS TIMESTAMP)", Timestamp.valueOf("2020-11-01 01:01:01")),
            arguments(
                "SELECT * from (SELECT CAST('2020-11-01 01:01:01' AS TIMESTAMP))",
                Timestamp.valueOf("2020-11-01 01:01:01")
            ),
            arguments(
                "SELECT CAST('1970.1.2' AS DATE)", "1970-01-02"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("getArguments")
    public void testDateTimeFormat(String sql, Object result) throws SQLException {
        assertThat(sqlHelper.querySingleValue(sql)).isEqualTo(result);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "now()",
        "current_timestamp",
        "current_timestamp()",
    })
    public void testNow(String fun) throws SQLException {
        String sql = "select " + fun;
        assertThat((Timestamp) sqlHelper.querySingleValue(sql))
            .isCloseTo(Timestamp.valueOf(LocalDateTime.now()), 3000L);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "curdate()",
        "current_date",
        "current_date()",
    })
    public void testCurDate(String fun) throws SQLException {
        String sql = "select " + fun;
        assertThat(sqlHelper.querySingleValue(sql).toString())
            .isEqualTo(LocalDate.now().toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "current_time",
        "current_time()",
        "curtime()",
    })
    public void testCurTime(String fun) throws SQLException {
        String sql = "select " + fun;
        assertThat(Time.valueOf((String) sqlHelper.querySingleValue(sql)))
            .isCloseTo(Time.valueOf(LocalTime.now()), 3000L);
    }

    @ParameterizedTest
    @CsvSource({
        "2022-04-14 00:00:00, 2022-04-14 00:00:00",
        "2022/04/14 00:00:00, 2022-04-14 00:00:00",
        "2022-05-15 00:14:01, 2022-05-15 00:14:01",
    })
    public void testUnixTimeStamp(String dateTime, String result) throws SQLException {
        String sql = "select unix_timestamp('" + dateTime + "')";
        assertThat(sqlHelper.querySingleValue(sql)).isEqualTo(Timestamp.valueOf(result).getTime() / 1000L);
    }

    @ParameterizedTest
    @ValueSource(longs = {
        0,
        1652544841,
    })
    public void testUnixTimeStamp1(long timestamp) throws SQLException {
        String sql = "select unix_timestamp(" + timestamp + ")";
        assertThat(sqlHelper.querySingleValue(sql)).isEqualTo(timestamp);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "now()",
        "current_timestamp()",
        "current_date() || ' ' || current_time()",
    })
    public void testUnixTimeStamp2(String expr) throws SQLException {
        String sql = "select unix_timestamp(" + expr + ")";
        assertThat((long) sqlHelper.querySingleValue(sql))
            .isCloseTo(Timestamp.valueOf(LocalDateTime.now()).getTime() / 1000L, Offset.offset(3L));
    }

    @ParameterizedTest
    @CsvSource({
        "2007-12-29, 2007-12-30, -1",
        "2007-12-29, 2007-12-30, -1",
        "2022-04-14, 2022-05-31, -47",
        "2022-04-30, 2022-05-01, -1",
        "2022-04-30, 2022-05-31, -31",
        "2022-05-31, 2022-04-13, 48",
        "2022-05-31, 2022-05-01, 30",
    })
    public void testDateDiff(String value1, String value2, long result) throws SQLException {
        String sql = "select datediff('" + value1 + "', '" + value2 + "')";
        assertThat(sqlHelper.querySingleValue(sql)).isEqualTo(result);
    }

    @ParameterizedTest
    @ValueSource(longs = {
        0,
        1649770110,
    })
    public void testFromUnixTime(long timestamp) throws SQLException {
        String sql = "select cast (from_unixtime(" + timestamp + ") as timestamp(3))";
        assertThat(((Timestamp) sqlHelper.querySingleValue(sql)).getTime() / 1000L).isEqualTo(timestamp);
    }
}
