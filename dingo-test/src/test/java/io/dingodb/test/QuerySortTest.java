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

import io.dingodb.common.table.TupleSchema;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

@Slf4j
public class QuerySortTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-create.sql");
        sqlHelper.execFile("/table-test-data.sql");
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
    public void testSort() throws SQLException {
        String sql = "select * from test order by id asc";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "1, Alice, 3.5\n"
                + "2, Betty, 4.0\n"
                + "3, Cindy, 4.5\n"
                + "4, Doris, 5.0\n"
                + "5, Emily, 5.5\n"
                + "6, Alice, 6.0\n"
                + "7, Betty, 6.5\n"
                + "8, Alice, 7.0\n"
                + "9, Cindy, 7.5\n"
        );
    }

    @Test
    public void testSort1() throws SQLException {
        String sql = "select * from test order by name desc, amount";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "5, Emily, 5.5\n"
                + "4, Doris, 5.0\n"
                + "3, Cindy, 4.5\n"
                + "9, Cindy, 7.5\n"
                + "2, Betty, 4.0\n"
                + "7, Betty, 6.5\n"
                + "1, Alice, 3.5\n"
                + "6, Alice, 6.0\n"
                + "8, Alice, 7.0\n"
        );
    }

    @Test
    public void testSortLimitOffset() throws SQLException {
        String sql = "select * from test order by name desc, amount limit 3 offset 2";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"id", "name", "amount"},
            TupleSchema.ofTypes("INTEGER", "STRING", "DOUBLE"),
            "3, Cindy, 4.5\n"
                + "9, Cindy, 7.5\n"
                + "2, Betty, 4.0\n"
        );
    }
}
