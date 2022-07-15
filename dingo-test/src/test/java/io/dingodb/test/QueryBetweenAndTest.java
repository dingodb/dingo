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

import io.dingodb.common.type.DingoTypeFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class QueryBetweenAndTest {
    private static final String TEST_ALL_DATA1
        = "4, a\n"
        + "5, b\n"
        + "6, c\n"
        + "7, a\n"
        + "8, b\n"
        + "9, c\n"
        + "10, a\n"
        + "11, b\n"
        + "12, c\n"
        + "13, a\n";

    private static final String TEST_ALL_DATA2
        = "5, b\n"
        + "6, c\n"
        + "7, a\n"
        + "8, b\n"
        + "9, c\n"
        + "10, a\n"
        + "11, b\n"
        + "12, c\n";

    private static final String TEST_ALL_DATA3
        = "1, a\n"
        + "2, b\n"
        + "3, c\n"
        + "4, a\n"
        + "5, b\n"
        + "6, c\n"
        + "7, a\n"
        + "8, b\n"
        + "9, c\n"
        + "10, a\n"
        + "11, b\n"
        + "12, c\n"
        + "13, a\n"
        + "14, b\n"
        + "15, c\n";

    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-between-create.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execFile("/table-test-between-data.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    @Test
    public void testScan1() throws SQLException, IOException {
        sqlHelper.queryTest("select * from t_ba where id between 4 and 13",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            TEST_ALL_DATA1
        );
    }

    @Test
    public void testScan2() throws SQLException, IOException {
        sqlHelper.queryTest("select * from t_ba where id < 13 and id > 4",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            TEST_ALL_DATA2
        );
    }

    @Test
    @Disabled
    public void testScan3() throws SQLException, IOException {
        sqlHelper.queryTest("select * from t_ba where name between 'a' and 'c'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            TEST_ALL_DATA3
        );
    }
}
