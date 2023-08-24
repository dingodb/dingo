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

package io.dingodb.test.dsl.run;

import io.dingodb.test.dsl.builder.SqlBuildingContext;
import io.dingodb.test.dsl.builder.SqlTableInfo;
import io.dingodb.test.dsl.builder.SqlTestCase;
import io.dingodb.test.dsl.builder.SqlTestCaseBuilder;
import io.dingodb.test.dsl.run.exec.StepConverter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SqlTestRunner {
    private Connection connection;
    private List<SqlRunningContext> runningContexts;
    private String name;

    protected SqlTestRunner() {
    }

    private static @NonNull TableInfo convertSqlTableInfo(
        @NonNull SqlTableInfo sqlTableInfo,
        @NonNull StepConverter converter
    ) {
        return new TableInfo(
            converter.visit(sqlTableInfo.getCreateStep()),
            converter.visit(sqlTableInfo.getInitStep())
        );
    }

    private @NonNull SqlTest convertTestCase(
        @NonNull SqlTestCase testCase,
        @NonNull StepConverter stepConverter
    ) {
        return new SqlTest(
            name + ": " + testCase.getName(),
            stepConverter.visitEach(testCase.getSteps()),
            testCase.isEnabled(),
            testCase.getTableMapping(),
            testCase.getModifiedTableIds()
        );
    }

    protected void setup() throws Exception {
    }

    protected void cleanUp() {
    }

    protected abstract Connection getConnection() throws Exception;

    @BeforeAll
    public void setupAll() throws Exception {
        setup();
        connection = getConnection();
        runningContexts = new LinkedList<>();
    }

    @AfterAll
    public void cleanUpAll() throws SQLException {
        for (SqlRunningContext context : runningContexts) {
            context.cleanUp();
        }
        connection.close();
        cleanUp();
    }

    public Stream<DynamicTest> getTests(@NonNull SqlTestCaseBuilder builder) {
        name = builder.getName();
        SqlBuildingContext context = builder.getContext();
        StepConverter stepConverter = StepConverter.of(context.getCallerClass(), context.getBasePath());
        SqlRunningContext runningContext = new SqlRunningContext(connection);
        runningContexts.add(runningContext);
        for (Map.Entry<String, SqlTableInfo> entry : context.getTables().entrySet()) {
            SqlTableInfo info = entry.getValue();
            runningContext.put(entry.getKey(), convertSqlTableInfo(info, stepConverter));
        }
        Stream<SqlTestCase> cases;
        if (context.getCases().stream().anyMatch(SqlTestCase::isOnly)) {
            cases = context.getCases().stream().peek(c -> c.setEnabled(c.isOnly()));
        } else {
            cases = context.getCases().stream();
        }
        return cases
            .map(c -> convertTestCase(c, stepConverter))
            .map(t -> DynamicTest.dynamicTest(t.getName(), () -> t.run(runningContext)));
    }
}
