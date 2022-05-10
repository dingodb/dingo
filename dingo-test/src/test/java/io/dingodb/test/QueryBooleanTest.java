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

import java.sql.SQLException;

public class QueryBooleanTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-boolean-create.sql");
        sqlHelper.execFile("/table-test-boolean-data.sql");
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
    public void test() throws SQLException {
        String sql = "select * from bldemo";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "age", "amount", "address", "is_delete"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "DOUBLE", "STRING", "BOOLEAN"),
            "1, c1, 28, 109.325, beijing, true\n"
                + "2, c2, 17, 139.2, beijing, false\n"
                + "3, c3, 22, 34.89, shanghai, false\n"
                + "4, c4, 33, 3392.88, zhengzhou, true\n"
        );
    }
}
