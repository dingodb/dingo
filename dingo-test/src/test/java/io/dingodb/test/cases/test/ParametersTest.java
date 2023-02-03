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

package io.dingodb.test.cases.test;

import io.dingodb.test.SqlHelper;
import io.dingodb.test.cases.ClassTestMethod;
import io.dingodb.test.cases.provider.ParametersCasesJUnit5;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ParametersTest {
    @Getter
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
        new ParametersCasesJUnit5().getByKeys(sqlHelper.getConnection());
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ArgumentsSource(ParametersCasesJUnit5.class)
    public void test(String ignored, @NonNull ClassTestMethod method) throws Exception {
        method.getMethod().run(sqlHelper.getConnection());
    }
}
