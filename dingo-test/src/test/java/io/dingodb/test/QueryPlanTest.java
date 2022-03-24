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

package io.dingodb.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

@Disabled
@Slf4j
public class QueryPlanTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-create.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() {
    }

    @AfterEach
    public void cleanUp() {
    }

    @Test
    public void testExplainSimpleValues() throws SQLException {
        String sql = "explain plan for select 1";
        sqlHelper.explainTest(
            sql,
            "EnumerableRoot",
            "DingoValues"
        );
    }

    @Test
    public void testExplainInsertValues() throws SQLException {
        String sql = "explain plan for insert into test values(1, 'Alice', 1.0)";
        sqlHelper.explainTest(
            sql,
            "EnumerableRoot",
            "DingoCoalesce",
            "DingoExchange",
            "DingoPartModify",
            "DingoDistributedValues"
        );
    }

    @Test
    public void testExplainScan() throws SQLException {
        String sql = "explain plan for select * from dingo.test";
        sqlHelper.explainTest(
            sql,
            "EnumerableRoot",
            "DingoCoalesce",
            "DingoExchange",
            "DingoPartScan"
        );
    }
}
