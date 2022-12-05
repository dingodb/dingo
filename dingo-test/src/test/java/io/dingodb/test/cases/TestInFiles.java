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
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestInFiles {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    public static @NonNull Stream<Arguments> cases() {
        return Stream.of(
            arguments(ImmutableList.of(
                "double/create.sql",
                "double/data_with_null.sql",
                "select_all.sql",
                "double/data_with_null_all.csv"
            )),
            arguments(ImmutableList.of(
                "boolean/create.sql",
                "boolean/data_of_int.sql",
                "boolean/select_true.sql",
                "boolean/data_of_true.csv"
            )),
            arguments(ImmutableList.of(
                "date_key/create.sql",
                "date_key/data.sql",
                "select_all.sql",
                "date_key/data_all.csv"
            )),
            arguments(ImmutableList.of(
                "strings/create.sql",
                "strings/data_with_null.sql",
                "strings/select_concat_all.sql",
                "strings/data_with_null_concat_all.csv"
            )),
            arguments(ImmutableList.of(
                "string_int_double/create.sql",
                "string_int_double/data.sql",
                "string_int_double/select_pow_all.sql",
                "string_int_double/data_pow_all.csv",
                "string_int_double/select_mod_all.sql",
                "string_int_double/data_mod_all.csv"
            )),
            arguments(ImmutableList.of(
                "string_int_double_map/create.sql",
                "string_int_double_map/data.sql",
                "string_int_double_map/update.sql",
                "string_int_double_map/select_scalar.sql",
                "string_int_double_map/data_scalar.csv"
            ))
        );
    }

    public static @NonNull Stream<Arguments> tempCases() {
        return Stream.of(
            arguments(ImmutableList.of(
                "array/create.sql",
                "array/data.sql",
                "array/select_array_item_all.sql",
                "array/data_array_item_all.csv"
            ))
        );
    }

    @ParameterizedTest
    @MethodSource({"tempCases"})
    public void testTemp(List<String> fileNames) throws SQLException, IOException {
        sqlHelper.randomTable().doTestFiles(this.getClass(), fileNames);
    }

    @ParameterizedTest
    @MethodSource({"cases"})
    public void test(List<String> fileNames) throws SQLException, IOException {
        sqlHelper.randomTable().doTestFiles(this.getClass(), fileNames);
    }
}
