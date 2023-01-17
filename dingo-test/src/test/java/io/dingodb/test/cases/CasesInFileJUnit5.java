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

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class CasesInFileJUnit5 implements ArgumentsProvider {
    public static @NonNull Arguments fileCase(String name, String... fileNames) {
        return arguments(name, Arrays.stream(fileNames)
            .map(InputTestFile::fromFileName)
            .collect(Collectors.toList())
        );
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
            fileCase(
                "Insert and select",
                "string_double/create.sql",
                "string_double/data.sql",
                "select_all.sql",
                "string_double/data.csv"
            ),
            fileCase(
                "With null",
                "double/create.sql",
                "double/data_with_null.sql",
                "select_all.sql",
                "double/data_with_null_all.csv"
            ),
            fileCase(
                "Cast int to boolean",
                "boolean/create.sql",
                "boolean/data_of_int.sql",
                "boolean/select_true.sql",
                "boolean/data_of_true.csv"
            ),
            fileCase(
                "Date as primary key",
                "date_key/create.sql",
                "date_key/data.sql",
                "select_all.sql",
                "date_key/data_all.csv"
            ),
            fileCase(
                "Concat null",
                "strings/create.sql",
                "strings/data_with_null.sql",
                "strings/select_concat_all.sql",
                "strings/data_with_null_concat_all.csv"
            ),
            fileCase(
                "Function 'pow'",
                "string_int_double/create.sql",
                "string_int_double/data.sql",
                "string_int_double/select_pow_all.sql",
                "string_int_double/data_pow_all.csv",
                "string_int_double/select_mod_all.sql",
                "string_int_double/data_mod_all.csv"
            ),
            fileCase(
                "Map",
                "string_int_double_map/create.sql",
                "string_int_double_map/data.sql",
                "string_int_double_map/update.sql",
                "string_int_double_map/select_scalar.sql",
                "string_int_double_map/data_scalar.csv"
            ),
            fileCase(
                "Array",
                "array/create.sql",
                "array/data.sql",
                "array/select_array_item_all.sql",
                "array/data_array_item_all.csv"
            ),
            fileCase(
                "Double as primary key",
                "double_pm/create.sql",
                "double_pm/data.sql",
                "select_all.sql",
                "double_pm/data.csv"
            ),
            fileCase("Update using function",
                "string_int_double_string/create.sql",
                "string_int_double_string/data.sql",
                "string_int_double_string/update.sql",
                "select_all.sql",
                "string_int_double_string/data_updated.csv"
            )
        );
    }
}
