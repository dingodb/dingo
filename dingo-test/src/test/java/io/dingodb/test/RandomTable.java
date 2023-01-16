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

import io.dingodb.test.asserts.Assert;
import io.dingodb.test.cases.InputTestFile;
import org.apache.commons.io.IOUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import javax.annotation.Nonnull;

public class RandomTable {
    private static final String TABLE_NAME_PLACEHOLDER = "table";

    private final SqlHelper sqlHelper;
    private final String name;
    private final int index;

    public RandomTable(SqlHelper sqlHelper) {
        this(sqlHelper, 0);
    }

    public RandomTable(SqlHelper sqlHelper, int index) {
        this.sqlHelper = sqlHelper;
        this.name = SqlHelper.randomTableName();
        this.index = index;
    }

    @Override
    public String toString() {
        return getName();
    }

    public @Nonnull String getName() {
        return name + (index > 0 ? "_" + index : "");
    }

    private @Nonnull String getPlaceholder() {
        return "{" + TABLE_NAME_PLACEHOLDER + (index > 0 ? "_" + index : "") + "}";
    }

    private @NonNull String transSql(@NonNull String sql) {
        return sql.replace(getPlaceholder(), getName());
    }

    public PreparedStatement prepare(String sql) throws SQLException {
        return sqlHelper.getConnection().prepareStatement(transSql(sql));
    }

    public void execSql(Statement statement, String sql) throws SQLException {
        SqlHelper.execSql(statement, transSql(sql));
    }

    public void execSqls(@Nonnull String... sqlStrings) throws SQLException {
        try (Statement statement = sqlHelper.getConnection().createStatement()) {
            for (String sql : sqlStrings) {
                execSql(statement, transSql(sql));
            }
        }
    }

    public void execFiles(String @NonNull ... fileNames) throws SQLException, IOException {
        try (Statement statement = sqlHelper.getConnection().createStatement()) {
            for (String fileName : fileNames) {
                InputTestFile file = InputTestFile.fromFileName(fileName);
                if (file.getType() == InputTestFile.Type.SQL) {
                    String sql = IOUtils.toString(file.getStream(), StandardCharsets.UTF_8);
                    execSql(statement, transSql(sql));
                }
            }
        }
    }

    public void drop() throws SQLException {
        sqlHelper.dropTable(getName());
    }

    public void doTestFiles(
        @NonNull List<InputTestFile> files
    ) throws SQLException, IOException {
        try (Statement statement = sqlHelper.getConnection().createStatement()) {
            for (InputTestFile file : files) {
                if (file.getType() == InputTestFile.Type.SQL) {
                    String sql = IOUtils.toString(file.getStream(), StandardCharsets.UTF_8);
                    execSql(statement, transSql(sql));
                } else if (file.getType() == InputTestFile.Type.CSV) {
                    ResultSet resultSet = statement.getResultSet();
                    Assert.resultSet(resultSet).asInCsv(file.getStream());
                    resultSet.close();
                }
            }
        }
        sqlHelper.dropTable(getName());
    }

    public void queryTest(String sql, String[] columns, List<Object[]> tuples) throws SQLException {
        sqlHelper.queryTest(transSql(sql), columns, tuples);
    }
}
