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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class QueryTimestampWithPrecision {
    private static final String TEST_ALL_DATA
        = "1, 2022-06-20 11:49:05.632, 11:50:05.740\n"
        + "2, 2022-06-20 11:51:23.770, 11:52:23.620\n"
        + "3, 2022-06-20 11:52:43.800, 11:53:43.800\n";

    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-time-timestamp-create.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execFile("/table-test-time-timestamp-data.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    @Test
    public void testScan() throws SQLException, IOException {
        sqlHelper.queryTest("select * from t_test",
            new String[]{"id", "create_datetime", "update_time"},
            TupleSchema.ofTypes("INTEGER", "TIMESTAMP", "TIME"),
            TEST_ALL_DATA
        );
    }
}
