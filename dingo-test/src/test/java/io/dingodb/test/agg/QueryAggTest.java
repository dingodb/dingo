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
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

@Slf4j
public class QueryAggTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile(QueryAggTest.class.getResourceAsStream("table-test-create.sql"));
        sqlHelper.execFile(QueryAggTest.class.getResourceAsStream("table-test-data.sql"));
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
    public void testCount() throws SQLException, JsonProcessingException {
        String sql = "select name, count(*) from test group by name";
        sqlHelper.queryTest(
            sql,
            new String[]{"name", "expr$1"},
            DingoTypeFactory.tuple("STRING", "LONG"),
            "Alice, 3\n"
                + "Betty, 2\n"
                + "Cindy, 2\n"
                + "Doris, 1\n"
                + "Emily, 1\n"
        );
    }

    @Test
    public void testCount1() throws SQLException, JsonProcessingException {
        String sql = "select count(*) from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"expr$0"},
            DingoTypeFactory.tuple("LONG"),
            "9\n"
        );
    }

    @Test
    public void testDistinct() throws SQLException, JsonProcessingException {
        String sql = "select distinct name from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"name"},
            DingoTypeFactory.tuple("STRING"),
            "Alice\n"
                + "Betty\n"
                + "Cindy\n"
                + "Doris\n"
                + "Emily\n"
        );
    }

    @Test
    public void testDistinctCnt() throws SQLException, JsonProcessingException {
        String sql = "select count(distinct name) from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"expr$0"},
            DingoTypeFactory.tuple("LONG"),
            "5\n"
        );
    }

    @Test
    public void testDistinctWithGroupBy() throws SQLException, JsonProcessingException {
        String sql = "select name, count(distinct id) from test group by name";
        sqlHelper.queryTest(
            sql,
            new String[]{"name", "expr$1"},
            DingoTypeFactory.tuple("STRING", "LONG"),
            "Alice, 3\n"
                + "Betty, 2\n"
                + "Cindy, 2\n"
                + "Doris, 1\n"
                + "Emily, 1\n");
    }

    @Test
    public void testMultiCountWithGroupBy() throws SQLException, JsonProcessingException {
        String sql = "select name, count(distinct id), count(distinct name) from test group by name";
        sqlHelper.queryTest(
            sql,
            new String[]{"name", "expr$1", "expr$2"},
            DingoTypeFactory.tuple("STRING", "LONG", "LONG"),
            "Alice, 3, 1\n"
                + "Betty, 2, 1\n"
                + "Cindy, 2, 1\n"
                + "Doris, 1, 1\n"
                + "Emily, 1, 1\n");
    }

    @Test
    public void testMultiCountWithDiffColumns() throws SQLException, JsonProcessingException {
        String sql = "select count(distinct id), count(distinct name) from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"expr$0", "expr$1"},
            DingoTypeFactory.tuple("LONG", "LONG"),
            "9, 5\n");
    }

    @Test
    public void testMulAggWithCountMax() throws SQLException, JsonProcessingException {
        String sql = "select count(distinct name), max(id) from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"expr$0", "expr$1"},
            DingoTypeFactory.tuple("LONG", "INTEGER"),
            "5, 9\n");
    }

    @Test
    public void testCount2() throws SQLException, JsonProcessingException {
        String sql = "select count(*) from test group by name";
        sqlHelper.queryTest(
            sql,
            new String[]{"expr$0"},
            DingoTypeFactory.tuple("LONG"),
            "3\n"
                + "2\n"
                + "2\n"
                + "1\n"
                + "1\n"
        );
    }

    @Test
    public void testSum1() throws SQLException, JsonProcessingException {
        String sql = "select sum(amount) as all_sum from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"all_sum"},
            DingoTypeFactory.tuple("DOUBLE"),
            "49.5\n"
        );
    }

    @Test
    public void testMin() throws SQLException, JsonProcessingException {
        String sql = "select min(amount) as min_amount from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"min_amount"},
            DingoTypeFactory.tuple("DOUBLE"),
            "3.5\n"
        );
    }

    @Test
    public void testMax() throws SQLException, JsonProcessingException {
        String sql = "select max(amount) as max_amount from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"max_amount"},
            DingoTypeFactory.tuple("DOUBLE"),
            "7.5\n"
        );
    }

    @Test
    public void testAvg() throws SQLException, JsonProcessingException {
        String sql = "select avg(amount) as avg_amount from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"avg_amount"},
            DingoTypeFactory.tuple("DOUBLE"),
            "5.5\n"
        );
    }

    @Test
    public void testAvg1() throws SQLException, JsonProcessingException {
        String sql = "select name, avg(amount) as avg_amount from test group by name";
        sqlHelper.queryTest(
            sql,
            new String[]{"name", "avg_amount"},
            DingoTypeFactory.tuple("STRING", "DOUBLE"),
            "Alice, 5.5\n"
                + "Betty, 5.25\n"
                + "Cindy, 6.0\n"
                + "Doris, 5.0\n"
                + "Emily, 5.5"
        );
    }

    @Test
    public void testAvg2() throws SQLException, JsonProcessingException {
        String sql = "select name, avg(id) as avg_id, avg(amount) as avg_amount from test group by name";
        sqlHelper.queryTest(
            sql,
            new String[]{"name", "avg_id", "avg_amount"},
            DingoTypeFactory.tuple("STRING", "INTEGER", "DOUBLE"),
            "Alice, 5, 5.5\n"
                + "Betty, 4, 5.25\n"
                + "Cindy, 6, 6.0\n"
                + "Doris, 4, 5.0\n"
                + "Emily, 5, 5.5"
        );
    }

    @Test
    public void testSumAvg() throws Exception {
        String sql = "select sum(amount), avg(amount), count(amount) from test";
        sqlHelper.queryTest(
            sql,
            new String[]{"expr$0", "expr$1", "expr$2"},
            DingoTypeFactory.tuple("DOUBLE", "DOUBLE", "LONG"),
            "49.5, 5.5, 9"
        );
    }
}
