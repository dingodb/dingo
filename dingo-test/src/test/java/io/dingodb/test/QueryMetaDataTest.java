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

import io.dingodb.calcite.DingoRootSchema;
import io.dingodb.meta.MetaService;
import io.dingodb.test.asserts.Assert;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

@Slf4j
public class QueryMetaDataTest {
    private static final String SCHEMA_NAME = DingoRootSchema.DEFAULT_SCHEMA_NAME;

    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
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
        try (ResultSet resultSet = metaData.getSchemas()) {
            Assert.resultSet(resultSet)
                .columnLabels(
                    new String[]{"TABLE_SCHEM", "TABLE_CATALOG"}
                )
                .isRecords(Collections.singletonList(
                    new Object[]{MetaService.DINGO_NAME, null}
                ));
        }
    }

    @Test
    public void testGetSchemasWithPattern() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        try (ResultSet resultSet = metaData.getSchemas(null, "D%")) {
            Assert.resultSet(resultSet)
                .columnLabels(
                    new String[]{"TABLE_SCHEM", "TABLE_CATALOG"}
                )
                .isRecords(Collections.singletonList(
                    new Object[]{DingoRootSchema.DEFAULT_SCHEMA_NAME, null}
                ));
        }
    }

    @Test
    public void testGetTables() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        try (ResultSet resultSet = metaData.getTables(null, SCHEMA_NAME, null, null)) {
            Assert.resultSet(resultSet)
                .columnLabels(
                    new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"}
                )
                .isRecords(Arrays.asList(
                    new Object[]{null, SCHEMA_NAME, "TEST", "TABLE"},
                    new Object[]{null, SCHEMA_NAME, "TEST1", "TABLE"}
                ));
        }
    }

    @Test
    public void testGetTablesWithPattern() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        try (ResultSet resultSet = metaData.getTables(null, SCHEMA_NAME, "TEST_", null)) {
            Assert.resultSet(resultSet)
                .columnLabels(
                    new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"}
                )
                .isRecords(Collections.singletonList(
                    new Object[]{null, SCHEMA_NAME, "TEST1", "TABLE"}
                ));
        }
    }

    @Test
    public void testGetTablesNullSchema() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        try (ResultSet resultSet = metaData.getTables(null, null, "%", null)) {
            Assert.resultSet(resultSet)
                .columnLabels(
                    new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"}
                )
                .isRecords(Arrays.asList(
                    new Object[]{null, SCHEMA_NAME, "TEST", "TABLE"},
                    new Object[]{null, SCHEMA_NAME, "TEST1", "TABLE"}
                ));
        }
    }

    @Test
    public void testGetColumns() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        try (ResultSet resultSet = metaData.getColumns(null, SCHEMA_NAME, "TEST", null)) {
            Assert.resultSet(resultSet)
                .columnLabels(
                    new String[]{
                        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                        "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                        "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
                    }
                )
                .isRecords(Arrays.asList(
                    new Object[]{
                        null, SCHEMA_NAME, "TEST", "ID", 4,
                        "INTEGER NOT NULL", -1, null, 10, 0,
                        -1, 1, "NO", "", ""
                    },
                    new Object[]{
                        null, SCHEMA_NAME, "TEST", "NAME", 12,
                        "VARCHAR(32) NOT NULL", 32, null, 10, 0,
                        32, 2, "NO", "", ""
                    },
                    new Object[]{
                        null, SCHEMA_NAME, "TEST", "AMOUNT", 8,
                        "DOUBLE", -1, null, 10, 1,
                        -1, 3, "YES", "", ""
                    }
                ));
        }
    }

    @Test
    public void testGetColumnsWithPattern() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        try (ResultSet resultSet = metaData.getColumns(null, SCHEMA_NAME, "TEST", "N%E")) {
            Assert.resultSet(resultSet)
                .columnLabels(
                    new String[]{
                        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                        "TYPE_NAME", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                        "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
                    }
                )
                .isRecords(Collections.singletonList(
                    new Object[]{
                        null, SCHEMA_NAME, "TEST", "NAME", 12,
                        "VARCHAR(32) NOT NULL", 32, null, 10, 0,
                        32, 2, "NO", "", ""
                    }
                ));
        }
    }

    @Test
    public void testGetPrimaryKeys() throws SQLException {
        DatabaseMetaData metaData = sqlHelper.metaData();
        try (ResultSet resultSet = metaData.getPrimaryKeys(null, SCHEMA_NAME, "TEST")) {
            Assert.resultSet(resultSet)
                .columnLabels(
                    new String[]{
                        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ"
                    }
                )
                .isRecords(Collections.singletonList(
                    new Object[]{
                        null, SCHEMA_NAME, "TEST", "ID", (short) 1,
                    }
                ));
        }
    }
}
