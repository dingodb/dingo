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

public class TransferCases extends SqlTestCaseJavaBuilder {
    protected TransferCases() {
        super("Transfer Table");
    }

    @Override
    protected void build() {
        table("i4k_vs_f80", file("cases/tables/i4k_vs_f80.create.sql"))
            .init(file("cases/tables/i4k_vs_f80.data.sql"));

        table("i4k_vsk_lk_vs_f80", file("i4k_vsk_lk_vs_f80/create.sql"));

        test("Transfer")
            .use("table0", "i4k_vs_f80")
            .use("table1", "i4k_vsk_lk_vs_f80")
            .modify("i4k_vsk_lk_vs_f80")
            .step(
                "insert into {table1}" +
                    " select id, name, amount > 6.0, name, amount+1.0 from {table0} where amount > 5.0",
                count(5)
            )
            .data("table1", csv(
                "id0, id1, id2, name, amount",
                "INTEGER, STRING, BOOLEAN, STRING, DOUBLE",
                "5, Emily, false, Emily, 6.5",
                "6, Alice, false, Alice, 7.0",
                "7, Betty, true, Betty, 7.5",
                "8, Alice, true, Alice, 8.0",
                "9, Cindy, true, Cindy, 8.5"
            ));
    }
}
