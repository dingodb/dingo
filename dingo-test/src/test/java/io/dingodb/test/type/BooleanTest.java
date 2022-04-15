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

package io.dingodb.test.type;

import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.Services;
import io.dingodb.meta.test.MetaTestService;
import io.dingodb.test.SqlHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class BooleanTest {
    private static Connection connection;
    private static SqlHelper sqlHelper;


    @BeforeAll
    public static void setupAll() throws Exception {
        connection = (sqlHelper = new SqlHelper()).getConnection();
        sqlHelper.execFile("/table-test4-support-boolean-creation.sql");
        sqlHelper.execFile("/table-test4-support-boolean-insert.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        connection.close();
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).clear();
    }

    @Test
    public void testFullScan() throws SQLException {
        String sql = "select id, name, sex from Test4 where id = 1";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"id", "name", "sex"},
            TupleSchema.ofTypes("INTEGER", "STRING", "BOOLEAN"),
            "1, Jimmy, true"
        );
    }

    @Test
    public void testFilter() throws SQLException {
        String sql = "SELECT * from Test4 where sex = true";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"id", "name", "sex"},
            TupleSchema.ofTypes("INTEGER", "STRING", "BOOLEAN"),
            "1, Jimmy, true"
        );
    }

    @Test
    public void testNot() throws SQLException {
        String sql = "SELECT * from Test4 where sex <> true";
        sqlHelper.queryTestOrder(
            sql,
            new String[]{"id", "name", "sex"},
            TupleSchema.ofTypes("INTEGER", "STRING", "BOOLEAN"),
            "2, Alex, false"
        );
    }
}
