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

package io.dingodb.test.update;

import io.dingodb.common.table.TupleSchema;
import io.dingodb.test.SqlHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class UpdateTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
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
    public void testQuery() throws SQLException, IOException {
        sqlHelper.execFile("/table-update-test5-creation.sql");
        sqlHelper.execFile("/table-update-test5-insert.sql");
        System.out.println("Hello world");
        String sql = "update strdemo set address=replace(address,'beijing','shanghai')";
        int count = sqlHelper.execSqlCmd(sql);
        System.out.println("count: " + count);
        sql = "select id, name, age, amount, address from strdemo where address = 'shanghai'";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "age", "amount", "address"},
            TupleSchema.ofTypes("INTEGER", "STRING", "INTEGER", "DOUBLE", "STRING"),
            "1, a, 10, 1.0, shanghai"
        );
        sqlHelper.clearTable("strdemo");
    }
}
