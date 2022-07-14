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

package io.dingodb.test.agg;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.test.SqlHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class QueryAgg3Test {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile(QueryAgg2Test.class.getResourceAsStream("table-datetest-create.sql"));
        sqlHelper.execFile(QueryAgg2Test.class.getResourceAsStream("table-datetest-data.sql"));
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    @Test
    public void testMinDate() throws SQLException, JsonProcessingException {
        sqlHelper.queryTestInOrderWithApproxTime(
            "select min(birthday) from datetest",
            new String[]{"expr$0"},
            DingoTypeFactory.tuple("DATE"),
            "1949-01-01"
        );
    }

    @Test
    public void testMaxDate() throws SQLException, JsonProcessingException {
        sqlHelper.queryTestInOrderWithApproxTime(
            "select max(birthday) from datetest",
            new String[]{"expr$0"},
            DingoTypeFactory.tuple("DATE"),
            "2022-03-04"
        );
    }

    @Test
    public void testMinTime() throws SQLException, JsonProcessingException {
        sqlHelper.queryTest(
            "select min(create_time) from datetest",
            new String[]{"expr$0"},
            DingoTypeFactory.tuple("TIME"),
            "00:30:08"
        );
    }

    @Test
    public void testMaxTime() throws SQLException, JsonProcessingException {
        sqlHelper.queryTest(
            "select max(create_time) from datetest",
            new String[]{"expr$0"},
            DingoTypeFactory.tuple("TIME"),
            "19:00:00"
        );
    }

    @Test
    public void testMinTimeStamp() throws SQLException, JsonProcessingException {
        sqlHelper.queryTest(
            "select min(update_time) from datetest",
            new String[]{"expr$0"},
            DingoTypeFactory.tuple("TIMESTAMP"),
            "1952-12-31 12:12:12"
        );
    }

    @Test
    public void testMaxTimeStamp() throws SQLException, JsonProcessingException {
        sqlHelper.queryTest(
            "select max(update_time) from datetest",
            new String[]{"expr$0"},
            DingoTypeFactory.tuple("TIMESTAMP"),
            "2022-12-01 01:02:03"
        );
    }
}
