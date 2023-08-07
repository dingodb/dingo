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

public class DateTimeQueryCases extends SqlTestCaseJavaBuilder {
    protected DateTimeQueryCases() {
        super("Date & Time");
    }

    @Override
    protected void build() {
        table("i4k_vs_dt0", file("i4k_vs_dt0/create.sql"))
            .init(file("i4k_vs_dt0/data.sql"), 2);

        table("i4k_vs_tm0", file("i4k_vs_tm0/create.sql"))
            .init(file("i4k_vs_tm0/data.sql"), 2);

        table("i4k_vs_ts0", file("i4k_vs_ts0/create.sql"))
            .init(file("i4k_vs_ts0/data.sql"), 2);

        table("dtk_i40", file("dtk_i40/create.sql"))
            .init(file("dtk_i40/data.sql"), 2);

        table("i4k_i40_vs0_f40_dt0_ts0", file("i4k_i40_vs0_f40_dt0_ts0/create.sql"))
            .init(file("i4k_i40_vs0_f40_dt0_ts0/data.sql"), 3);

        table(
            "i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0",
            file("i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0/create.sql")
        ).init(file("i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0/data.sql"), 21);

        test("Date")
            .use("table", "i4k_vs_dt0")
            .step(
                "select * from {table}",
                csv(file("i4k_vs_dt0/data.csv"))
            );

        test("Time")
            .use("table", "i4k_vs_tm0")
            .step(
                "select * from {table}",
                csv(file("i4k_vs_tm0/data.csv"))
            );

        test("Timestamp")
            .use("table", "i4k_vs_ts0")
            .step(
                "select * from {table}",
                csv(file("i4k_vs_ts0/data.csv"))
            );

        test("Concat date")
            .use("table", "i4k_vs_dt0")
            .step(
                "select 'test-' || birth from {table} where id = 1",
                csv("EXPR$0", "STRING", "test-2020-01-01")
            );

        test("Concat time")
            .use("table", "i4k_vs_tm0")
            .step(
                "select 'test-' || birth from {table} where id = 1",
                csv("EXPR$0", "STRING", "test-00:00:01")
            );

        test("Concat int-string-time")
            .use("table", "i4k_vs_ts0")
            .step(
                "select id || name || birth from {table} where id = 1",
                csv("EXPR$0", "STRING", "1Alice2020-01-01 00:00:01")
            );

        test("Date as primary key")
            .use("table", "dtk_i40")
            .data(csv(file("dtk_i40/data.csv")));

        test("Select nothing")
            .use("table", "i4k_i40_vs0_f40_dt0_ts0")
            .step(
                "select * from {table} where card_no=23 and `account`=14",
                csv(
                    "ID,CARD_NO,NAME,ACCOUNT,TIME_DATE,TIME_DATETIME",
                    "INT,INT,STRING,FLOAT,DATE,TIMESTAMP"
                )
            );

        test("Select by timestamp = now()")
            .use("table", "i4k_i40_vs0_f40_dt0_ts0")
            .step(
                "select * from {table} where time_datetime=now()",
                csv(
                    "ID,CARD_NO,NAME,ACCOUNT,TIME_DATE,TIME_DATETIME",
                    "INT,INT,STRING,FLOAT,DATE,TIMESTAMP"
                )
            );

        test("Aggregation min of DATE")
            .use("table", "i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0")
            .step(
                "select min(birthday) from {table}",
                csv(
                    "expr$0",
                    "STRING",
                    "1949-01-01"
                )
            );

        test("Aggregation max of DATE")
            .use("table", "i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0")
            .step(
                "select max(birthday) from {table}",
                csv(
                    "expr$0",
                    "STRING",
                    "2022-07-07"
                )
            );

        test("Aggregation min of TIME")
            .use("table", "i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0")
            .step(
                "select min(create_time) from {table}",
                csv(
                    "expr$0",
                    "STRING",
                    "00:00:00"
                )
            );

        test("Aggregation max of TIME")
            .use("table", "i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0")
            .step(
                "select max(create_time) from {table}",
                csv(
                    "expr$0",
                    "STRING",
                    "22:10:10"
                )
            );

        test("Aggregation min of TIMESTAMP")
            .use("table", "i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0")
            .step(
                "select min(update_time) from {table}",
                csv(
                    "expr$0",
                    "TIMESTAMP",
                    "1952-12-31 12:12:12"
                )
            );

        test("Aggregation max of TIMESTAMP")
            .use("table", "i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0")
            .step(
                "select max(update_time) from {table}",
                csv(
                    "expr$0",
                    "TIMESTAMP",
                    "2024-05-04 12:00:00"
                )
            );
    }
}
