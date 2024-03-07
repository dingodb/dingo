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

package io.dingodb.test.dsl;

import io.dingodb.test.dsl.builder.SqlTestCaseJavaBuilder;

import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class BasicDmlCases extends SqlTestCaseJavaBuilder {
    protected BasicDmlCases() {
        super("Basic DML");
    }

    @Override
    protected void build() {
        table("i4k_vs_f80", file("cases/tables/i4k_vs_f80.create.sql"))
            .init(file("cases/tables/i4k_vs_f80.data.sql"), 9);

        table("i4k_i80_f80", file("i4k_i80_f80/create.sql"))
            .init(file("i4k_i80_f80/data.sql"), 1);

        table("i8k_i80_f80", file("i8k_i80_f80/create.sql"))
            .init(file("i4k_i80_f80/data.sql"), 1);

        table("i4k_vs0_i40_f80_vs0", file("i4k_vs0_i40_f80_vs0/create.sql"))
            .init(file("i4k_vs0_i40_f80_vs0/data.sql"), 9);

        test("Update by primary key")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step("update {table} set amount = 100 where id = 1", count(1))
            .data(csv(
                "ID, NAME, AMOUNT",
                "INT, STRING, DOUBLE",
                "1, Alice, 100.0",
                "2, Betty, 4.0",
                "3, Cindy, 4.5",
                "4, Doris, 5.0",
                "5, Emily, 5.5",
                "6, Alice, 6.0",
                "7, Betty, 6.5",
                "8, Alice, 7.0",
                "9, Cindy, 7.5"
            ));

        test("Update by `or` of primary keys")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step("update {table} set amount = 100.23 where id = 3 or id = 5", count(2))
            .data(csv(
                "ID, NAME, AMOUNT",
                "INT, STRING, DOUBLE",
                "1, Alice, 3.5",
                "2, Betty, 4.0",
                "3, Cindy, 100.23",
                "4, Doris, 5.0",
                "5, Emily, 100.23",
                "6, Alice, 6.0",
                "7, Betty, 6.5",
                "8, Alice, 7.0",
                "9, Cindy, 7.5"
            ));

        test("Update with expression")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step("update {table} set amount = amount + 100", count(9))
            .data(csv(
                "ID, NAME, AMOUNT",
                "INT, STRING, DOUBLE",
                "1, Alice, 103.5",
                "2, Betty, 104.0",
                "3, Cindy, 104.5",
                "4, Doris, 105.0",
                "5, Emily, 105.5",
                "6, Alice, 106.0",
                "7, Betty, 106.5",
                "8, Alice, 107.0",
                "9, Cindy, 107.5"
            ));

        test("Update with conflicting conditions")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step(
                "update {table} set amount = 0.0 where name = 'Alice' and name = 'Betty'",
                count(0)
            )
            .data(csv(file("i4k_vs_f80/data.csv")));

        test("Delete by `or` of primary keys")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step(
                "delete from {table} where id = 3 or id = 4",
                count(2)
            )
            .data(csv(
                "ID, NAME, AMOUNT",
                "INT, STRING, DOUBLE",
                "1, Alice, 3.5",
                "2, Betty, 4.0",
                "5, Emily, 5.5",
                "6, Alice, 6.0",
                "7, Betty, 6.5",
                "8, Alice, 7.0",
                "9, Cindy, 7.5"
            ));

        test("Delete by column value.")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step(
                "delete from {table} where name = 'Alice'",
                count(3)
            )
            .data(csv(
                "ID, NAME, AMOUNT",
                "INT, STRING, DOUBLE",
                "2, Betty, 4.0",
                "3, Cindy, 4.5",
                "4, Doris, 5.0",
                "5, Emily, 5.5",
                "7, Betty, 6.5",
                "9, Cindy, 7.5"
            ));

        test("Delete with conflicting conditions")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step(
                "delete from {table} where name = 'Alice' and name = 'Betty'",
                count(0)
            )
            .data(csv(file("i4k_vs_f80/data.csv")));

        test("Insert")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step(
                "insert into {table} values(10, 'Alice', 8.0), (11, 'Cindy', 8.5)",
                count(2)
            )
            .data(csv(
                "ID, NAME, AMOUNT",
                "INT, STRING, DOUBLE",
                "1, Alice, 3.5",
                "2, Betty, 4.0",
                "3, Cindy, 4.5",
                "4, Doris, 5.0",
                "5, Emily, 5.5",
                "6, Alice, 6.0",
                "7, Betty, 6.5",
                "8, Alice, 7.0",
                "9, Cindy, 7.5",
                "10, Alice, 8.0",
                "11, Cindy, 8.5"
            ));

        test("Batch insert")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .custom(context -> {
                try (Statement statement = context.getStatement()) {
                    statement.addBatch(
                        context.transSql("insert into {table} values"
                            + "(14, 'Alice', 14.0)")
                    );
                    statement.addBatch(
                        context.transSql("insert into {table} values"
                            + "(15, 'Betty', 15.0),"
                            + "(16, 'Cindy', 16.0)")
                    );
                    int[] count = statement.executeBatch();
                    assertThat(count).isEqualTo(new int[]{1, 2});
                }
            })
            .data(csv(
                "ID, NAME, AMOUNT",
                "INT, STRING, DOUBLE",
                "1, Alice, 3.5",
                "2, Betty, 4.0",
                "3, Cindy, 4.5",
                "4, Doris, 5.0",
                "5, Emily, 5.5",
                "6, Alice, 6.0",
                "7, Betty, 6.5",
                "8, Alice, 7.0",
                "9, Cindy, 7.5",
                "14, Alice, 14.0",
                "15, Betty, 15.0",
                "16, Cindy, 16.0"
            ));

        test("Insert int to long")
            .use("table", "i4k_i80_f80")
            .data(csv(
                "ID, AMT, AMOUNT",
                "INT, LONG, DOUBLE",
                "1, 55, 23.45"
            ));

        test("Insert int to long key")
            .use("table", "i8k_i80_f80")
            .data(csv(
                "ID, AMT, AMOUNT",
                "LONG, LONG, DOUBLE",
                "1, 55, 23.45"
            ));

        test("Update long with int")
            .use("table", "i4k_i80_f80")
            .modify("i4k_i80_f80")
            .step("update {table} set amt = 15 where id = 1", count(1))
            .data(csv(
                "ID, AMT, AMOUNT",
                "INT, LONG, DOUBLE",
                "1, 15, 23.45"
            ));

        test("Update with function")
            .use("table", "i4k_vs0_i40_f80_vs0")
            .modify("i4k_vs0_i40_f80_vs0")
            .step("update {table} set address = replace(address, 'Beijing', 'Shanghai')", count(2))
            .data(csv(file("i4k_vs0_i40_f80_vs0/update.csv")));

        // In list with >= 20 elements is converted as join with values.
        test("In list with >=20 elements")
            .use("table", "i4k_vs0_i40_f80_vs0")
            .modify("i4k_vs0_i40_f80_vs0")
            .step(file("i4k_vs0_i40_f80_vs0/update_with_in_list.sql"), count(9))
            .data(csv(file("i4k_vs0_i40_f80_vs0/update_with_in_list.csv")));
    }
}
