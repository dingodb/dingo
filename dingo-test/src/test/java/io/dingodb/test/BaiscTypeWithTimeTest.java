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

import io.dingodb.calcite.Connections;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.Services;
import io.dingodb.meta.test.MetaTestService;
import io.dingodb.test.asserts.AssertResultSet;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


@Slf4j
public class BaiscTypeWithTimeTest {
    private static final String TEST_ALL_DATA
        = "1, Alice, 00:00:01\n";
    private static Connection connection;
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).init(null);
        Services.initNetService();
        connection = Connections.getConnection(MetaTestService.SCHEMA_NAME);
        sqlHelper = new SqlHelper(connection);
        sqlHelper.execUpdate("/table-test-create-with-time.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        connection.close();
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).clear();
    }

    private static void checkDatumInTable(
        String tableName,
        String[] columns,
        TupleSchema schema,
        String data
    ) throws SQLException {
        String sql = "select * from " + tableName;
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                AssertResultSet.of(resultSet).isRecords(columns, schema, data);
            }
        }
    }

    private static void checkDatumInTestTable(String data) throws SQLException {
        checkDatumInTable("test",
            new String[]{"id", "name", "birth"},
            TupleSchema.ofTypes("INTEGER", "STRING", "TIME"),
            data
        );
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execUpdate("/table-test-data-with-time.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {
        sqlHelper.clear("test");
    }

    @Test
    public void testScan() throws SQLException, IOException {
        checkDatumInTestTable(TEST_ALL_DATA);
    }
}
