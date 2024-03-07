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
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;

import static org.assertj.core.api.Assertions.assertThat;

public class ParameterDmlCases extends SqlTestCaseJavaBuilder {
    protected ParameterDmlCases() {
        super("Parameter DML");
    }

    @Override
    protected void build() {
        table("i4k_vs_f80", file("cases/tables/i4k_vs_f80.create.sql"));

        test("Insert")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .custom(context -> {
                String sql = "insert into {table} values(?, ?, ?)";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    statement.setInt(1, 10);
                    statement.setString(2, "Alice");
                    statement.setDouble(3, 10.0);
                    boolean b = statement.execute();
                    check(statement, b, sql).test(count(1));
                    statement.setInt(1, 11);
                    statement.setString(2, "Betty");
                    statement.setDouble(3, 11.0);
                    b = statement.execute();
                    check(statement, b, sql).test(count(1));
                }
            })
            .data(csv(
                "ID, NAME, AMOUNT",
                "INT, STRING, DOUBLE",
                "10, Alice, 10.0",
                "11, Betty, 11.0"
            ));

        test("Insert multiple values")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .custom(context -> {
                String sql = "insert into {table} values(?, ?, ?), (?, ?, ?), (?, ?, ?)";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    statement.setInt(1, 10);
                    statement.setString(2, "Alice");
                    statement.setDouble(3, 10.0);
                    statement.setInt(4, 11);
                    statement.setString(5, "Betty");
                    statement.setDouble(6, 11.0);
                    statement.setInt(7, 12);
                    statement.setString(8, "Cindy");
                    statement.setDouble(9, 12.0);
                    boolean b = statement.execute();
                    check(statement, b, sql).test(count(3));
                }
            })
            .data(csv(
                "ID, NAME, AMOUNT",
                "INT, STRING, DOUBLE",
                "10, Alice, 10.0",
                "11, Betty, 11.0",
                "12, Cindy, 12.0"
            ));

        test("Batch insert")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .custom(context -> {
                String sql = "insert into {table} values(?, ?, ?)";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    statement.setInt(1, 12);
                    statement.setString(2, "Alice");
                    statement.setDouble(3, 12.0);
                    statement.addBatch();
                    statement.setInt(1, 13);
                    statement.setString(2, "Betty");
                    statement.setDouble(3, 13.0);
                    statement.addBatch();
                    int[] count = statement.executeBatch();
                    assertThat(count).isEqualTo(new int[]{1, 1});
                }
            })
            .data(is(
                new String[]{"id", "name", "amount"},
                ImmutableList.of(
                    new Object[]{12, "Alice", 12.0},
                    new Object[]{13, "Betty", 13.0}
                )
            ));

        test("delete")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .step(file("cases/tables/i4k_vs_f80.data.sql"))
            .custom(context -> {
                String sql = "delete from {table} where name = ?";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    statement.setString(1, "Alice");
                    int count = statement.executeUpdate();
                    assertThat(count).isEqualTo(3);
                    statement.setString(1, "Betty");
                    count = statement.executeUpdate();
                    assertThat(count).isEqualTo(2);
                }
            })
            .data(is(
                new String[]{"id", "name", "amount"},
                ImmutableList.of(
                    new Object[]{3, "Cindy", 4.5},
                    new Object[]{4, "Doris", 5.0},
                    new Object[]{5, "Emily", 5.5},
                    new Object[]{9, "Cindy", 7.5}
                )
            ));

        test("Insert with date & time")
            .step(
                "create table {table} ("
                    + "id int,"
                    + "data date,"
                    + "data1 time,"
                    + "data2 timestamp,"
                    + "primary key(id)"
                    + ")"
            )
            .custom(context -> {
                String sql = "insert into {table} values(?, ?, ?, ?)";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    statement.setInt(1, 1);
                    statement.setDate(2, new Date(0));
                    statement.setTime(3, new Time(0));
                    statement.setTimestamp(4, new Timestamp(0));
                    boolean b = statement.execute();
                    check(statement, b, sql).test(count(1));
                    statement.setInt(1, 2);
                    statement.setDate(2, new Date(86400000));
                    statement.setTime(3, new Time(3600000));
                    statement.setTimestamp(4, new Timestamp(1));
                    b = statement.execute();
                    check(statement, b, sql).test(count(1));
                }
            })
            .data(is(
                new String[]{"id", "data", "data1", "data2"},
                ImmutableList.of(
                    new Object[]{1, new Date(0).toString(), new Time(0).toString(), new Timestamp(0)},
                    new Object[]{2, new Date(86400000).toString(), new Time(3600000).toString(), new Timestamp(1)}
                )
            ));
    }
}
