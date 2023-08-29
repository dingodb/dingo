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

package io.dingodb.test.dsl.builder;

import io.dingodb.test.dsl.builder.checker.SqlCsvFileResultChecker;
import io.dingodb.test.dsl.builder.checker.SqlCsvStringResultChecker;
import io.dingodb.test.dsl.builder.checker.SqlExceptionChecker;
import io.dingodb.test.dsl.builder.checker.SqlObjectResultChecker;
import io.dingodb.test.dsl.builder.checker.SqlResultCountChecker;
import io.dingodb.test.dsl.builder.checker.SqlResultDumper;
import io.dingodb.test.dsl.builder.checker.SqlUpdateCountChecker;
import io.dingodb.test.dsl.builder.step.SqlFileStep;
import io.dingodb.test.dsl.builder.step.SqlStringStep;
import io.dingodb.test.dsl.run.check.CheckContext;
import io.dingodb.test.utils.ResourceFileUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

@Slf4j
public abstract class SqlTestCaseJavaBuilder extends SqlTestCaseBuilder {
    protected SqlTestCaseJavaBuilder(String name) {
        super(name);
        context = new SqlBuildingContext();
        build();
    }

    public static @NonNull InputStream file(String fileName) {
        Class<?> clazz = ResourceFileUtils.getCallerClass();
        return ResourceFileUtils.getResourceFile(fileName, clazz);
    }

    public static @NonNull SqlCsvStringResultChecker csv(String... csvLines) {
        return new SqlCsvStringResultChecker(csvLines);
    }

    public static @NonNull SqlCsvFileResultChecker csv(InputStream csvFile) {
        return new SqlCsvFileResultChecker(csvFile);
    }

    public static @NonNull SqlObjectResultChecker is(String[] columnLabels, List<Object[]> tuples) {
        return new SqlObjectResultChecker(columnLabels, tuples);
    }

    public static @NonNull SqlUpdateCountChecker count(int updateCount) {
        return new SqlUpdateCountChecker(updateCount);
    }

    public static @NonNull SqlResultCountChecker rows(int rowCount) {
        return new SqlResultCountChecker(rowCount);
    }

    public static @NonNull SqlExceptionChecker exception(Class<? extends Exception> exception) {
        return new SqlExceptionChecker(exception, null, null, null);
    }

    public static @NonNull SqlExceptionChecker exception(Class<? extends Exception> exception, String contains) {
        return new SqlExceptionChecker(exception, contains, null, null);
    }

    public static @NonNull SqlExceptionChecker exception(@NonNull SqlExceptionStub stub) {
        return new SqlExceptionChecker(SQLException.class, null, stub.sqlCode, stub.sqlState);
    }

    public static @NonNull SqlExceptionChecker exception(@NonNull SqlExceptionStub stub, String contains) {
        return new SqlExceptionChecker(SQLException.class, contains, stub.sqlCode, stub.sqlState);
    }

    public static @NonNull SqlExceptionStub sql(int sqlCode, String sqlState) {
        return new SqlExceptionStub(sqlCode, sqlState);
    }

    @SuppressWarnings("unused")
    public static @NonNull SqlResultDumper dump() {
        return new SqlResultDumper();
    }

    public static @NonNull CheckContext check(
        Statement statement,
        boolean executeReturnedValue,
        String info
    ) throws SQLException {
        return new CheckContext(statement, executeReturnedValue, info, null);
    }

    public static @NonNull CheckContext check(
        Statement statement,
        boolean executeReturnedValue,
        String info,
        Exception exception
    ) throws SQLException {
        return new CheckContext(statement, executeReturnedValue, info, exception);
    }

    public SqlBuildingContext.TableContext table(String tableId, String createSqlString) {
        SqlTableInfo tableInfo = new SqlTableInfo(new SqlStringStep(createSqlString));
        return context.createTable(tableId, tableInfo);
    }

    public SqlBuildingContext.TableContext table(String tableId, InputStream createSqlFile) {
        SqlTableInfo tableInfo = new SqlTableInfo(new SqlFileStep(createSqlFile));
        return context.createTable(tableId, tableInfo);
    }

    public SqlBuildingContext.TestCaseContext test(@NonNull String name) {
        SqlTestCase testCase = new SqlTestCase(name);
        return context.addTestCase(testCase);
    }

    protected abstract void build();

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class SqlExceptionStub {
        private final int sqlCode;
        private final String sqlState;
    }
}
