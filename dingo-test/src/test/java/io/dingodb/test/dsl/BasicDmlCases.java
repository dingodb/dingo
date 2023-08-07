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

public class BasicDmlCases extends SqlTestCaseJavaBuilder {
    protected BasicDmlCases() {
        super("Basic DML");
    }

    @Override
    protected void build() {
        table("i4k_vs_f80", file("i4k_vs_f80/create.sql"))
            .init(file("i4k_vs_f80/data.sql"), 9);

        table("i4k_i80_f80", file("i4k_i80_f80/create.sql"))
            .init(file("i4k_i80_f80/data.sql"), 1);

        table("i8k_i80_f80", file("i8k_i80_f80/create.sql"))
            .init(file("i4k_i80_f80/data.sql"), 1);

        table("i4k_vs0_i40_f80_vs0", file("i4k_vs0_i40_f80_vs0/create.sql"))
            .init(file("i4k_vs0_i40_f80_vs0/data.sql"), 9);

        test("Update with conflicting conditions")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step(
                "update {table} set amount = 0.0 where name = 'Alice' and name = 'Betty'",
                count(0)
            )
            .data(csv(file("i4k_vs_f80/data.csv")));

        test("Delete with conflicting conditions")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step(
                "delete from {table} where name = 'Alice' and name = 'Betty'",
                count(0)
            )
            .data(csv(file("i4k_vs_f80/data.csv")));

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
