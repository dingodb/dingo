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

package io.dingodb.test.cases;

import io.dingodb.test.SqlHelper;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import static io.dingodb.test.cases.Case.SELECT_ALL;
import static io.dingodb.test.cases.Case.exec;
import static io.dingodb.test.cases.Case.file;

public class CasesTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @Test
    public void testTemp() throws Exception {
        Case.of(
            exec(file("string_double/create.sql")),
            exec("delete from {table} where name = 'Alice' and name = 'Betty'").updateCount(0),
            exec(SELECT_ALL).result(
                "ID, NAME, AMOUNT",
                "INT, STRING, DOUBLE"
            )
        ).run(sqlHelper.getConnection());
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ArgumentsSource(CasesProvider.class)
    public void test(String ignored, @NonNull Case testCase) throws Exception {
        testCase.run(sqlHelper.getConnection());
    }

    @ParameterizedTest(name = "StatementForEachStep [{index}] {0}")
    @ArgumentsSource(CasesProvider.class)
    public void testWithStatementForEachStep(String ignored, @NonNull Case testCase) throws Exception {
        testCase.runWithStatementForEachStep(sqlHelper.getConnection());
    }
}
