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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

public class CasesInFileTest {
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
            // In list with >= 20 elements is converted as join with values.
            CasesInFileJUnit5.fileCase("In list with >=20 elements",
                "string_int_double_string/create.sql",
                "string_int_double_string/data.sql",
                "string_int_double_string/update_1.sql",
                "select_all.sql",
                "string_int_double_string/data_updated_1.csv"
            )
        );
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @ArgumentsSource(CasesInFileJUnit5.class)
    public void test(String ignored, List<InputTestFile> files) throws SQLException, IOException {
        sqlHelper.randomTable().doTestFiles(files);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("getParametersTemp")
    public void testTemp(String ignored, List<InputTestFile> files) throws SQLException, IOException {
        test(ignored, files);
    }
}
