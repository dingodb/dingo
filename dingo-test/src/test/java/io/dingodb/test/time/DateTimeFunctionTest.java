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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat((Time) sqlHelper.querySingleValue(sql))
            .isCloseTo(Time.valueOf(LocalTime.now()), 3000L);
    }
}
