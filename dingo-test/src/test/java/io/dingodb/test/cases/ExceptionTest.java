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

import com.google.common.collect.ImmutableList;
import io.dingodb.test.SqlHelper;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ExceptionTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @NonNull
    public static Stream<Arguments> getParametersTemp() {
        return Stream.of(
            arguments("By `thrown` function", ImmutableList.of(
                "select throw(null)"
            ), 90002, "90002", false)
        );
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ArgumentsSource(ExceptionCasesJUnit5.class)
    public void testException(
        String ignored,
        @NonNull List<String> sqlList,
        int sqlCode,
        String sqlState,
        boolean needDropping
    ) {
        sqlHelper.exceptionTest(sqlList, needDropping, sqlCode, sqlState);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("getParametersTemp")
    public void testExceptionTemp(
        String testName,
        @NonNull List<String> sqlList,
        int sqlCode,
        String sqlState,
        boolean needDropping
    ) {
        testException(testName, sqlList, sqlCode, sqlState, needDropping);
    }
}
