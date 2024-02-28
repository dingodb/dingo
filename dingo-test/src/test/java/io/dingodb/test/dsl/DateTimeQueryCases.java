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

import com.google.common.collect.ImmutableList;
import io.dingodb.test.dsl.builder.SqlTestCaseJavaBuilder;

import java.sql.Timestamp;
import java.util.Collections;

public class DateTimeQueryCases extends SqlTestCaseJavaBuilder {
    protected DateTimeQueryCases() {
        super("Date & Time");
    }

    @Override
    protected void build() {
        table("i4k_tm0_ts0", file("i4k_tm0_ts0/create.sql"))
            .init(file("i4k_tm0_ts0/data.sql"), 3);

        table("i4k_ts0_tm0", file("i4k_ts0_tm0/create.sql"))
            .init(file("i4k_ts0_tm0/data.sql"), 3);

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

        table("i4k_vs0_i40_f80_dt0", file("i4k_vs0_i40_f80_dt0/create.sql"))
            .init(file("i4k_vs0_i40_f80_dt0/data.sql"), 1);

        table("i4k_vs0_i40_f80_tm0", file("i4k_vs0_i40_f80_tm0/create.sql"))
            .init(file("i4k_vs0_i40_f80_tm0/data.sql"), 1);

        table(
            "i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0",
            file("cases/tables/i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0.create.sql")
        ).init(file("cases/tables/i4k_vs0_i40_f80_vs0_dt0_tm0_ts0_l0.data.sql"), 21);

        table("i4k_vs0_i40_f80_vs0_tm0", file("i4k_vs0_i40_f80_vs0_tm0/create.sql"))
            .init(file("i4k_vs0_i40_f80_vs0_tm0/data.sql"), 1);

        table("i4k_vs_i40_f80_vs0_dt0_tm0_ts0", file("i4k_vs_i40_f80_vs0_dt0_tm0_ts0/create.sql"))
            .init(file("i4k_vs_i40_f80_vs0_dt0_tm0_ts0/data.sql"), 7);

        table("vsk_i40_vs0_dt0_tm0_ts0", file("vsk_i40_vs0_dt0_tm0_ts0/create.sql"))
            .init(file("vsk_i40_vs0_dt0_tm0_ts0/data.sql"), 1);

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

        test("Select all")
            .use("table", "i4k_vs0_i40_f80_tm0")
            .data(
                is(
                    new String[]{"id", "name", "age", "amount", "create_time"},
                    ImmutableList.of(new Object[]{1, "zhang san", 18, 1342.09, "11:23:41"})
                )
            );

        test("Select all 1")
            .use("table", "i4k_vs0_i40_f80_dt0")
            .data(
                is(
                    new String[]{"id", "name", "age", "amount", "create_date"},
                    ImmutableList.of(new Object[]{1, "zhang san", 18, 1342.09, "2020-01-01"})
                )
            );

        test("Select by primary key")
            .use("table", "i4k_tm0_ts0")
            .step(
                "SELECT * from {table} where id = 2",
                is(
                    new String[]{"id", "create_time", "update_time"},
                    ImmutableList.of(new Object[]{2, "01:13:06", Timestamp.valueOf("1949-10-01 01:00:00")})
                )
            );

        test("Select with projection")
            .use("table", "vsk_i40_vs0_dt0_tm0_ts0")
            .step(
                "select date_column, timestamp_column, datetime_column from {table}",
                is(
                    new String[]{"date_column", "timestamp_column", "datetime_column"},
                    ImmutableList.of(new Object[]{"2003-12-31", 1447430881, "2007-1-31 23:59:59"})
                )
            );

        test("Fun `date_format`")
            .use("table", "vsk_i40_vs0_dt0_tm0_ts0")
            .step(
                "select date_format(date_column, '%d %m %Y') as new_date_column from {table}",
                is(
                    new String[]{"new_date_column"},
                    ImmutableList.of(new Object[]{"31 12 2003"})
                )
            );

        test("Fun `time_format`")
            .use("table", "i4k_vs0_i40_f80_vs0_tm0")
            .step(
                "select name, time_format(update_time, '%H:%i:%s') time_out from {table}",
                is(
                    new String[]{"name", "time_out"},
                    ImmutableList.of(new Object[]{"aa", "17:38:28"})
                )
            );

        test("Fun `timestamp_format`")
            .use("table", "i4k_vs_i40_f80_vs0_dt0_tm0_ts0")
            .step(
                "select name, timestamp_format(update_time, '%Y/%m/%d %H.%i.%s') ts_out from {table} where id=1",
                is(
                    new String[]{"name", "ts_out"},
                    ImmutableList.of(new Object[]{"zhangsan", "2022/04/08 18.05.07"})
                )
            );

        test("Fun `from_unixtime`")
            .use("table", "vsk_i40_vs0_dt0_tm0_ts0")
            .step(
                "select from_unixtime(timestamp_column) as new_datetime_column from {table}",
                is(
                    new String[]{"new_datetime_column"},
                    ImmutableList.of(new Object[]{new Timestamp(1447430881L * 1000L)})
                )
            );

        test("Fun `unix_timestamp`")
            .use("table", "i4k_vs_i40_f80_vs0_dt0_tm0_ts0")
            .step(
                "select unix_timestamp(update_time) as ts from {table} where id = 1",
                is(
                    new String[]{"ts"},
                    ImmutableList.of(new Object[]{Timestamp.valueOf("2022-04-08 18:05:07").getTime() / 1000L})
                )
            );

        test("Fun `datediff`")
            .use("table", "vsk_i40_vs0_dt0_tm0_ts0")
            .step(
                "select datediff(date_column, date_column) as new_diff from {table}",
                is(
                    new String[]{"new_diff"},
                    ImmutableList.of(new Object[]{0L})
                )
            );

        test("Select DATE type")
            .use("table", "vsk_i40_vs0_dt0_tm0_ts0")
            .step(
                "select date_type_column from {table}",
                is(
                    new String[]{"date_type_column"},
                    ImmutableList.of(new Object[]{"2003-12-31"})
                )
            );

        test("Select TIME type")
            .use("table", "vsk_i40_vs0_dt0_tm0_ts0")
            .step(
                "select time_type_column from {table}",
                is(
                    new String[]{"time_type_column"},
                    Collections.singletonList(new Object[]{"12:12:12"})
                )
            );

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

        test("Precision of timestamp")
            .use("table", "i4k_ts0_tm0")
            .data(
                csv(
                    "id, create_datetime, update_time",
                    "INTEGER, TIMESTAMP, STRING",
                    "1, 2022-06-20 11:49:05.632, 11:50:05",
                    "2, 2022-06-20 11:51:23.770, 11:52:23",
                    "3, 2022-06-20 11:52:43.800, 11:53:43"
                )
            );
    }
}
