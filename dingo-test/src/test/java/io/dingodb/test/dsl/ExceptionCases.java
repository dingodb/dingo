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

import java.sql.SQLException;

public class ExceptionCases extends SqlTestCaseJavaBuilder {
    protected ExceptionCases() {
        super("Exception");
    }

    @Override
    protected void build() {
        table("i4k_vs_i40_f80_vs0_l0", file("i4k_vs_i40_f80_vs0_l0/create.sql"));

        test("Insert string to bool")
            .use("table", "i4k_vs_i40_f80_vs0_l0")
            .step(
                "insert into {table} values(1, 'c1', 28, 109.325, 'beijing', 'true')",
                exception(SQLException.class)
            );

        test("SQL Parse error")
            .step("select", exception(sql(51001, "51001")));
    }
}
