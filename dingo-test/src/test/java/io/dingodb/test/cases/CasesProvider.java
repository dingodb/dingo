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

import io.dingodb.calcite.DingoRootSchema;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.stream.Stream;

import static io.dingodb.test.cases.Case.exec;
import static io.dingodb.test.cases.Case.file;

public class CasesProvider implements ArgumentsProvider {
    private static final String SELECT_ALL = "select * from {table}";

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
            Case.of(
                "Create, insert and select",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec(SELECT_ALL)
                    .result(file("string_double/data.csv"))
            ),
            Case.of(
                "Table name with schema",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from " + DingoRootSchema.DEFAULT_SCHEMA_NAME + ".{table}")
                    .result(file("string_double/data.csv"))
            ),
            Case.of(
                "Select filtered",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where amount > 4.0")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, DOUBLE",
                        "3, Cindy, 4.5",
                        "4, Doris, 5.0",
                        "5, Emily, 5.5",
                        "6, Alice, 6.0",
                        "7, Betty, 6.5",
                        "8, Alice, 7.0",
                        "9, Cindy, 7.5"
                    )
            ),
            Case.of(
                "Select projected",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select name as label, amount * 10.0 as score from {table}")
                    .result(
                        "label, score",
                        "STRING, DOUBLE",
                        "Alice, 35",
                        "Betty, 40",
                        "Cindy, 45",
                        "Doris, 50",
                        "Emily, 55",
                        "Alice, 60",
                        "Betty, 65",
                        "Alice, 70",
                        "Cindy, 75"
                    )
            ),
            Case.of(
                "Get by primary key",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where id = 1")
                    .result(
                        "id, name, amount",
                        "INT, STRING, DOUBLE",
                        "1, Alice, 3.5"
                    )
            ),
            Case.of(
                "Get by `or` of primary key",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where id = 1 or id = 2")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, DOUBLE",
                        "1, Alice, 3.5",
                        "2, Betty, 4.0"
                    )
            ),
            Case.of(
                "Get by `in list` of primary key",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where id in (1, 2, 3)")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, DOUBLE",
                        "1, Alice, 3.5",
                        "2, Betty, 4.0",
                        "3, Cindy, 4.5"
                    )
            ),
            Case.of(
                "Select filtered by `not in list` of primary key",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where id not in (3, 4, 5, 6, 7, 8, 9)")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, DOUBLE",
                        "1, Alice, 3.5",
                        "2, Betty, 4.0"
                    )
            ),
            Case.of(
                "Select filtered by `and` of conditions",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where id > 1 and name = 'Alice' and amount > 6")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, DOUBLE",
                        "8, Alice, 7.0"
                    )
            ),
            Case.of(
                "With null",
                exec(file("double/create.sql")),
                exec(file("double/data_with_null.sql")).updateCount(2),
                exec(SELECT_ALL)
                    .result(file("double/data_with_null_all.csv"))
            ),
            Case.of(
                "Cast double to int",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select id, name, cast(amount as int) as amount from {table}")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, INTEGER",
                        "1, Alice, 4",
                        "2, Betty, 4",
                        "3, Cindy, 5",
                        "4, Doris, 5",
                        "5, Emily, 6",
                        "6, Alice, 6",
                        "7, Betty, 7",
                        "8, Alice, 7",
                        "9, Cindy, 8"
                    )
            ),
            Case.of(
                "Cast int to boolean",
                exec(file("boolean/create.sql")),
                exec(file("boolean/data_of_int.sql")).updateCount(2),
                exec(file("boolean/select_true.sql"))
                    .result(file("boolean/data_of_true.csv"))
            ),
            Case.of(
                "Date as primary key",
                exec(file("date_key/create.sql")),
                exec(file("date_key/data.sql")).updateCount(2),
                exec(SELECT_ALL)
                    .result(file("date_key/data_all.csv"))
            ),
            Case.of(
                "Concat null",
                exec(file("strings/create.sql")),
                exec(file("strings/data_with_null.sql")).updateCount(1),
                exec(file("strings/select_concat_all.sql"))
                    .result(file("strings/data_with_null_concat_all.csv"))
            ),
            Case.of(
                "Function `case`",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec(file("string_double/select_case_when.sql"))
                    .result(file("string_double/select_case_when.csv"))
            ),
            Case.of(
                "Function `case` with multi `when`",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec(file("string_double/select_case_when_1.sql"))
                    .result(file("string_double/select_case_when_1.csv"))
            ),
            Case.of(
                "Function `pow`",
                exec(file("string_int_double/create.sql")),
                exec(file("string_int_double/data.sql")).updateCount(2),
                exec(file("string_int_double/select_pow_all.sql"))
                    .result(file("string_int_double/data_pow_all.csv")),
                exec(file("string_int_double/select_mod_all.sql"))
                    .result(file("string_int_double/data_mod_all.csv"))
            ),
            Case.of(
                "Map",
                exec(file("string_int_double_map/create.sql")),
                exec(file("string_int_double_map/data.sql")).updateCount(1),
                exec(file("string_int_double_map/update.sql")).updateCount(1),
                exec(file("string_int_double_map/select_scalar.sql"))
                    .result(file("string_int_double_map/data_scalar.csv"))
            ),
            Case.of(
                "Array",
                exec(file("array/create.sql")),
                exec(file("array/data.sql")),
                exec(file("array/select_array_item_all.sql"))
                    .result(file("array/data_array_item_all.csv"))
            ),
            Case.of(
                "Double as primary key",
                exec(file("double_pm/create.sql")),
                exec(file("double_pm/data.sql")).updateCount(3),
                exec(SELECT_ALL)
                    .result(file("double_pm/data.csv"))
            ),
            Case.of(
                "Update using function",
                exec(file("string_int_double_string/create.sql")),
                exec(file("string_int_double_string/data.sql")).updateCount(9),
                exec(file("string_int_double_string/update.sql")).updateCount(2),
                exec(SELECT_ALL)
                    .result(file("string_int_double_string/data_updated.csv"))
            ),
            // In list with >= 20 elements is converted as join with values.
            Case.of(
                "In list with >=20 elements",
                exec(file("string_int_double_string/create.sql")),
                exec(file("string_int_double_string/data.sql")).updateCount(9),
                exec(file("string_int_double_string/update_1.sql")).updateCount(9),
                exec(SELECT_ALL)
                    .result(file("string_int_double_string/data_updated_1.csv"))
            ),
            Case.of(
                "Conflicting conditions",
                exec(file("string_double/create.sql")),
                exec(file("string_double/select_conflict.sql"))
                    .result(
                        "ID, NAME, AMOUNT",
                        "INT, STRING, DOUBLE"
                    )
            ),
            Case.of(
                "Nested indices",
                exec(file("index/create.sql")),
                exec(file("index/data.sql")),
                exec(
                    "alter TABLE {table} add index int_index2(CARD_NO);"
                        + "alter TABLE {table} add index char_index2(NAME);"
                        + "create index multi_index on {table} (card_no, account)"
                ),
                exec(
                    "select * from {table} where card_no=23 and account=14"
                ).result(
                    "ID,CARD_NO,NAME,ACCOUNT,TIME_DATE,TIME_DATETIME",
                    "INT,INT,STRING,FLOAT,DATE,TIMESTAMP"
                )
            )
        );
    }
}
