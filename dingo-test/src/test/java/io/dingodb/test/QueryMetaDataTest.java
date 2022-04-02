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

import io.dingodb.test.asserts.AssertResultSet;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

@Slf4j
public class QueryMetaDataTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws SQLException, IOException {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-create.sql");
        sqlHelper.execFile("/table-test1-create.sql");
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
    public void testGetSchemas() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        ResultSet resultSet = metaData.getSchemas();
        AssertResultSet.of(resultSet)
            .columnLabels(
                new String[]{"TABLE_SCHEM", "TABLE_CATALOG"}
            )
            .isRecords(Arrays.asList(
                new Object[]{"metadata", null},
                new Object[]{"TEST", null}
            ));
    }

    @Test
    public void testGetTables() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        ResultSet resultSet = metaData.getTables(null, "TEST", null, null);
        AssertResultSet.of(resultSet)
            .columnLabels(
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"}
            )
            .isRecords(Arrays.asList(
                new Object[]{null, "TEST", "TEST", "TABLE"},
                new Object[]{null, "TEST", "TEST1", "TABLE"}
            ));
    }

    @Test
    public void testGetTablesNullSchema() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        ResultSet resultSet = metaData.getTables(null, null, "%", null);
        AssertResultSet.of(resultSet)
            .columnLabels(
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"}
            )
            .isRecords(Arrays.asList(
                new Object[]{null, "TEST", "TEST", "TABLE"},
                new Object[]{null, "TEST", "TEST1", "TABLE"}
            ));
    }

    @Test
    public void testGetColumns() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        ResultSet resultSet = metaData.getColumns(null, "TEST", "TEST", null);
        AssertResultSet.of(resultSet)
            .columnLabels(
                new String[]{
                    "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                    "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                    "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
                }
            )
            .isRecords(Arrays.asList(
                new Object[]{
                    null, "TEST", "TEST", "ID", 4,
                    "INTEGER NOT NULL", -1, null, 10, 0,
                    -1, 1, "NO", "", ""
                },
                new Object[]{
                    null, "TEST", "TEST", "NAME", 12,
                    "VARCHAR(32) NOT NULL", 32, null, 10, 0,
                    32, 2, "NO", "", ""
                },
                new Object[]{
                    null, "TEST", "TEST", "AMOUNT", 8,
                    "DOUBLE NOT NULL", -1, null, 10, 0,
                    -1, 3, "NO", "", ""
                }
            ));
    }
}
