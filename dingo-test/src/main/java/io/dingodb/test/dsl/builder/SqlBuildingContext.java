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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.test.dsl.builder.checker.SqlChecker;
import io.dingodb.test.dsl.builder.checker.SqlUpdateCountChecker;
import io.dingodb.test.dsl.builder.step.CustomStep;
import io.dingodb.test.dsl.builder.step.SqlFileStep;
import io.dingodb.test.dsl.builder.step.SqlStringStep;
import io.dingodb.test.dsl.run.exec.Exec;
import io.dingodb.test.utils.TableUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SqlBuildingContext {
    @Getter
    private final Map<String, SqlTableInfo> tables;
    @Getter
    private final List<SqlTestCase> cases;

    @Getter
    @Setter
    private Class<?> callerClass;
    @Getter
    @Setter
    private String basePath;

    private SqlBuildingContext(
        Map<String, SqlTableInfo> tables,
        List<SqlTestCase> cases,
        Class<?> callerClass,
        String basePath
    ) {
        this.tables = tables;
        this.cases = cases;
        this.callerClass = callerClass;
        this.basePath = basePath;
    }

    SqlBuildingContext() {
        this(new HashMap<>(1), new LinkedList<>(), null, null);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SqlBuildingContext(
        @JsonProperty("tables") Map<String, SqlTableInfo> tables,
        @JsonProperty("cases") List<SqlTestCase> cases
    ) {
        this(tables, cases, null, null);
    }

    public TableContext createTable(@NonNull String tableId, @NonNull SqlTableInfo sqlTableInfo) {
        tables.put(tableId, sqlTableInfo);
        return new TableContext(sqlTableInfo);
    }

    public TestCaseContext addTestCase(@NonNull SqlTestCase sqlTestCase) {
        cases.add(sqlTestCase);
        return new TestCaseContext(sqlTestCase);
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TableContext {
        private final SqlTableInfo tableInfo;

        public void init(String initSqlString) {
            tableInfo.setInitStep(new SqlStringStep(initSqlString));
        }

        public void init(String initSqlString, int updateCount) {
            tableInfo.setInitStep(new SqlStringStep(initSqlString, new SqlUpdateCountChecker(updateCount)));
        }

        public void init(InputStream initSqlFile) {
            tableInfo.setInitStep(new SqlFileStep(initSqlFile));
        }

        public void init(InputStream initSqlFile, int updateCount) {
            tableInfo.setInitStep(new SqlFileStep(initSqlFile, new SqlUpdateCountChecker(updateCount)));
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TestCaseContext {
        private final SqlTestCase testCase;

        public TestCaseContext step(String sqlString) {
            return step(sqlString, null);
        }

        public TestCaseContext step(String sqlString, SqlChecker checker) {
            testCase.getSteps().add(new SqlStringStep(sqlString, checker));
            return this;
        }

        public TestCaseContext step(InputStream sqlFile) {
            return step(sqlFile, null);
        }

        public TestCaseContext step(InputStream sqlFile, SqlChecker checker) {
            testCase.getSteps().add(new SqlFileStep(sqlFile, checker));
            return this;
        }

        public TestCaseContext data(SqlChecker checker) {
            return data(TableUtils.DEFAULT_TABLE_PLACEHOLDER_NAME, checker);
        }

        public TestCaseContext data(String tablePalaceHolderName, SqlChecker checker) {
            return step("select * from {" + tablePalaceHolderName + "}", checker);
        }

        public TestCaseContext custom(Exec exec) {
            testCase.getSteps().add(new CustomStep(exec));
            return this;
        }

        @SuppressWarnings("unused")
        public TestCaseContext skip() {
            testCase.setEnabled(false);
            return this;
        }

        @SuppressWarnings("unused")
        public TestCaseContext only() {
            testCase.setOnly(true);
            return this;
        }

        public TestCaseContext use(String placeHolder, String tableId) {
            testCase.getTableMapping().put(placeHolder, tableId);
            return this;
        }

        public TestCaseContext modify(String... tableId) {
            testCase.getModifiedTableIds().addAll(Arrays.asList(tableId));
            return this;
        }
    }
}
