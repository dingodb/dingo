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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class DefaultValueCases extends SqlTestCaseJavaBuilder {
    protected DefaultValueCases() {
        super("DefaultValue");
    }

    @Override
    protected void build() {
        table("i4k_vs_f80", file("cases/tables/i4k_vs_f80.create.sql"));

        table("i4k_vs_f80_def", file("i4k_vs_f80/create_with_default.sql"));

        table("i4k_vs_i40_def", file("i4k_vs_i40/create_with_default.sql"));

        table("i4k_vs_i40_f80_vs0", file("i4k_vs_i40_f80_vs0/create.sql"));

        test("Insert default DOUBLE")
            .use("table", "i4k_vs_f80_def")
            .modify("i4k_vs_f80_def")
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .data(csv(
                "id, name, amount",
                "INTEGER, STRING, DOUBLE",
                "100, lala, 1.0"
            ));

        test("Insert no default DOUBLE")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .data(csv(
                "id, name, amount",
                "INTEGER, STRING, DOUBLE",
                "100, lala, null"
            ));

        test("Insert default INT")
            .use("table", "i4k_vs_i40_def")
            .modify("i4k_vs_i40_def")
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .data(csv(
                "id, name, score",
                "INTEGER, STRING, INT",
                "100, lala, 100"
            ))
            .step("update {table} set score = score - 10", count(1))
            .data(csv(
                "id, name, score",
                "INTEGER, STRING, INT",
                "100, lala, 90"
            ));

        test("Insert multiple nullable columns")
            .use("table", "i4k_vs_i40_f80_vs0")
            .modify("i4k_vs_i40_f80_vs0")
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .data(csv(
                "id, name, age, amount, address",
                "INTEGER, STRING, INT, DOUBLE, STRING",
                "100, lala, null, null, null"
            ));

        test("Insert nullable column with default value")
            .step(
                "create table {table} (\n"
                    + "    id int not null,\n"
                    + "    name varchar(32) not null,\n"
                    + "    age int null default 20,\n"
                    + "    primary key(id)\n"
                    + ")"
            )
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .data(csv(
                "id, name, age",
                "INTEGER, STRING, INT",
                "100, lala, 20"
            ));

        test("Default of function `lcase`")
            .step(
                "create table {table} (\n"
                    + "    id int,\n"
                    + "    name varchar(32) not null,\n"
                    + "    address varchar(32) default lcase('ABC'),\n"
                    + "    primary key(id)\n"
                    + ")"
            )
            .step(
                "insert into {table}(id, name) values (100, 'lala')",
                count(1)
            )
            .data(csv(
                "id, name, address",
                "INT, STRING, STRING",
                "100, lala, abc"
            ));

        test("Default of function `concat`")
            .step(
                "create table {table} (\n"
                    + "    id int,\n"
                    + "    name varchar(32) not null,\n"
                    + "    address varchar(32) default concat('AAA','BBB'),\n"
                    + "    primary key(id)\n"
                    + ")"
            )
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .data(csv(
                "id, name, address",
                "INT, STRING, STRING",
                "100, lala, AAABBB"
            ));

        test("Multiple default null")
            .step(
                "create table {table} (\n"
                    + "    id int,\n"
                    + "    name varchar(32) not null,\n"
                    + "    age int default null, \n"
                    + "    address varchar(32) default null,\n"
                    + "    primary key(id)\n"
                    + ")"
            )
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .data(csv(
                "id, name, age, address",
                "INT, STRING, INT, STRING",
                "100, lala, null, null"
            ));

        test("Default with timestamp")
            .step(
                "create table {table} (\n"
                    + "    id int,\n"
                    + "    name varchar(32) not null,\n"
                    + "    age int default null,\n"
                    + "    address varchar(32) default null,\n"
                    + "    birthday timestamp not null default '2020-01-01 00:00:00',\n"
                    + "    primary key(id)\n"
                    + ")"
            )
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .data(csv(
                "id, name, age, address, birthday",
                "INT, STRING, INT, STRING, TIMESTAMP",
                "100, lala, null, null, 2020-01-01 00:00:00"
            ));

        test("Default with date, time, timestamp")
            .step(
                "create table {table} (\n"
                    + "    id int,\n"
                    + "    name varchar(32) not null,\n"
                    + "    age int default null,\n"
                    + "    address varchar(32) default null,\n"
                    + "    birth1 date not null default '2020-01-01',\n"
                    + "    birth2 time not null default '10:30:30',\n"
                    + "    birth3 timestamp not null default '2020-01-01 10:30:30',\n"
                    + "    primary key(id)\n"
                    + ")"
            )
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .data(csv(
                "id, name, age, address, birth1, birth2, birth3",
                "INT, STRING, INT, STRING, STRING, STRING, TIMESTAMP",
                "100, lala, null, null, 2020-01-01, 10:30:30, 2020-01-01 10:30:30"
            ));

        test("Update multiple nullable columns")
            .step(
                "create table {table} (\n"
                    + "    id int,\n"
                    + "    name varchar(32),\n"
                    + "    age int,\n"
                    + "    address varchar(32),\n"
                    + "    birth1 date,\n"
                    + "    birth2 time,\n"
                    + "    birth3 timestamp,\n"
                    + "    primary key(id)\n"
                    + ")"
            )
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .step("update {table} set address = 'beijing' where id = 100", count(1))
            .data(csv(
                "id, name, age, address, birth1, birth2, birth3",
                "INT, STRING, INT, STRING, STRING, STRING, TIMESTAMP",
                "100, lala, null, beijing, null, null, null"
            ))
            .step("update {table} set birth1 = '2020-11-11' where id = 100", count(1))
            .data(csv(
                "id, name, age, address, birth1, birth2, birth3",
                "INT, STRING, INT, STRING, STRING, STRING, TIMESTAMP",
                "100, lala, null, beijing, 2020-11-11, null, null"
            ))
            .step("update {table} set birth2 = '11:11:11' where id = 100", count(1))
            .data(csv(
                "id, name, age, address, birth1, birth2, birth3",
                "INT, STRING, INT, STRING, STRING, STRING, TIMESTAMP",
                "100, lala, null, beijing, 2020-11-11, 11:11:11, null"
            ))
            .step("update {table} set birth3 = '2022-11-11 11:11:11' where id = 100", count(1))
            .data(csv(
                "id, name, age, address, birth1, birth2, birth3",
                "INT, STRING, INT, STRING, STRING, STRING, TIMESTAMP",
                "100, lala, null, beijing, 2020-11-11, 11:11:11, 2022-11-11 11:11:11"
            ));

        test("Update multiple nullable columns 1")
            .step(
                "create table {table} (\n"
                    + "    id int,\n"
                    + "    name varchar(32),\n"
                    + "    birth1 date,\n"
                    + "    birth2 time,\n"
                    + "    birth3 timestamp,\n"
                    + "    primary key(id)\n"
                    + ")"
            )
            .step("insert into {table}(id, name) values (100, 'lala')", count(1))
            .step("update {table} set birth1 = current_date() where id = 100", count(1))
            .step("update {table} set birth2 = current_time() where id = 100", count(1))
            .step("update {table} set birth3 = now() where id = 100", count(1))
            .data(is(
                new String[]{"id", "name", "birth1", "birth2", "birth3"},
                ImmutableList.of(
                    new Object[]{
                        100,
                        "lala",
                        Date.valueOf(LocalDate.now()).toString(),
                        Time.valueOf(LocalTime.now()),
                        Timestamp.valueOf(LocalDateTime.now())
                    }
                )
            ).timeDeviation(20000L).timestampDeviation(20000L));
    }
}
