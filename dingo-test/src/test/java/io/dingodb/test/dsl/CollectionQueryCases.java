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

public class CollectionQueryCases extends SqlTestCaseJavaBuilder {
    protected CollectionQueryCases() {
        super("Collection");
    }

    @Override
    protected void build() {
        table("i4k_ai40", file("i4k_ai40/create.sql"))
            .init(file("i4k_ai40/data.sql"), 1);

        table("i4k_vs0_i40_f80_adt0", file("i4k_vs0_i40_f80_adt0/create.sql"))
            .init(file("i4k_vs0_i40_f80_adt0/data.sql"), 4);

        table("i4k_vs0_i40_f80_atm0", file("i4k_vs0_i40_f80_atm0/create.sql"))
            .init(file("i4k_vs0_i40_f80_atm0/data.sql"), 4);

        table("i4k_ats0", file("i4k_ats0/create.sql"))
            .init(file("i4k_ats0/data.sql"), 2);

        table("i4k_vs0_i40_f80_mp0", file("i4k_vs0_i40_f80_mp0/create.sql"))
            .init(file("i4k_vs0_i40_f80_mp0/data.sql"), 1);

        test("Array of int")
            .use("table", "i4k_ai40")
            .step(
                "select data[1] as d1, data[2] as d2, data[3] as d3 from {table}",
                csv(
                    "d1, d2, d3",
                    "INT, INT, INT",
                    "1, 2, 3"
                )
            );

        test("Array of date")
            .use("table", "i4k_vs0_i40_f80_adt0")
            .step(
                "select id, name, age, amount, run_inter[1] from {table}",
                csv(
                    "id, name, age, amount, expr$4",
                    "INT, STRING, INT, DOUBLE, STRING",
                    "1, zhangsan, 18, 23.5, 1987-07-16",
                    "2, lisi, 25, 895.89, 1999-12-12",
                    "3, wangwu, 55, 123.123, 2001-01-01",
                    "4, LiuJi, 100, 0.0, 1949-10-01"
                )
            );

        test("Array of time")
            .use("table", "i4k_vs0_i40_f80_atm0")
            .step(
                "select id, name, age, amount, test_time[1] from {table}",
                csv(
                    "id, name, age, amount, expr$4",
                    "INT, STRING, INT, DOUBLE, STRING",
                    "1, zhangsan, 18, 23.5, 10:11:12",
                    "2, lisi, 25, 895.89, 23:59:59",
                    "3, wangwu, 55, 123.123, 17:49:00",
                    "4, LiuJi, 100, 0.0, 00:00:10"
                )
            );

        test("Array of timestamp")
            .use("table", "i4k_ats0")
            .step(
                "select id, send_time[1] from {table}",
                csv(
                    "id, expr$1",
                    "INT, LONG",
                    "1, 1669101174",
                    "2, 1"
                )
            );

        test("Map")
            .use("table", "i4k_vs0_i40_f80_mp0")
            .step(
                file("i4k_vs0_i40_f80_mp0/select_scalar.sql"),
                csv(file("i4k_vs0_i40_f80_mp0/select_scalar.csv"))
            );
    }
}
