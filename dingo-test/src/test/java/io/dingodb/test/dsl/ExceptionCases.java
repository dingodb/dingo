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
import org.apache.calcite.rel.core.Aggregate;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExceptionCases extends SqlTestCaseJavaBuilder {
    protected ExceptionCases() {
        super("Exception");
    }

    @Override
    protected void build() {
        table("i4k_vs_i40_f80_vs0_l0", file("i4k_vs_i40_f80_vs0_l0/create.sql"));
        table("i4k_vs0_i40_f80_vs0", file("i4k_vs0_i40_f80_vs0/create.sql"))
            .init(file("i4k_vs0_i40_f80_vs0/data.sql"));

        test("Insert string to bool")
            .use("table", "i4k_vs_i40_f80_vs0_l0")
            .step(
                "insert into {table} values(1, 'c1', 28, 109.325, 'beijing', 'true')",
                exception(SQLException.class)
            );

        // DDL error
        test("SQL Parse error")
            .step("select", exception(sql(51001, "51001")));

        test("Illegal expression in context")
            .step("insert into {table} (1)", exception(sql(51002, "51002")));

        test("Unknown identifier")
            .step(
                "create table {table} (id int, data bomb, primary key(id))",
                exception(sql(52001, "52001"))
            );

        test("Table already exists")
            .use("table", "i4k_vs_i40_f80_vs0_l0")
            .step(
                "create table {table} (id int, primary key(id))",
                exception(sql(52002, "52002"))
            );

        // Validation error
        test("Create without primary key")
            .step("create table {table} (id int)", exception(sql(52003, "52003")));

        test("Missing column list")
            .step("create table {table}", exception(sql(52004, "52004")));

        test("Table not found")
            .step("select * from {table}", exception(sql(53001, "53001")));

        test("Column not found")
            .use("table", "i4k_vs_i40_f80_vs0_l0")
            .step("select not_exist from {table}", exception(sql(53002, "53002")));

        test("Column not allow null")
            .step("create table {table} (id int, primary key(id))")
            .step("insert into {table} values(null)", exception(sql(53003, "53003")));

        test("Column not allow null (array)")
            .step("create table {table} (id int, data varchar array not null, primary key(id))")
            .step("insert into {table} values(1, null)", exception(sql(53003, "53003")));

        test("Column not allow null (map)")
            .step("create table {table} (id int, data map not null, primary key(id))")
            .step("insert into {table} values(1, null)", exception(sql(53003, "53003")));

        test("Number format error")
            .step("create table {table} (id int, data int, primary key(id))")
            .step("insert into {table} values(1, 'abc')", exception(sql(53004, "53004")));

        test("Illegal use of dynamic parameter")
            .custom((context) -> {
                String sql = "select ?";
                SQLException e = assertThrows(SQLException.class, () -> {
                    try (PreparedStatement statement = context.getConnection().prepareStatement(sql)) {
                        statement.execute();
                    }
                });
                assertThat(e.getErrorCode()).isEqualTo(54001);
                assertThat(e.getSQLState()).isEqualTo("54001");
            });

        // Execution error
        test("Task failed")
            .step("create table {table} (id int, data double, primary key(id))")
            .step("insert into {table} values (1, 3.5)", count(1))
            .step("update {table} set data = 'abc'", exception(sql(60000, "60000")));

        // Unknown
        test("Cast float to int")
            .step("select cast(18293824503.55 as int) ca", exception(sql(90001, "90001")));

        // Intentionally
        test("By `thrown` function")
            .step("select throw(null)", exception(sql(90002, "90002")));

        // Other
        test("Type mismatch")
            .use("table", "i4k_vs0_i40_f80_vs0")
            .custom(context -> {
                // Disable assert in `Aggregate` to allow our own check in `DingoAggregate`.
                // but `gradle test` seems not respect to this and throws AssertionError occasionally.
                Aggregate.class.getClassLoader().clearAssertionStatus();
                Aggregate.class.getClassLoader().setClassAssertionStatus(Aggregate.class.getName(), false);
                assertFalse(Aggregate.class.desiredAssertionStatus());
            })
            .step(
                "select avg(name) from {table}",
                exception(sql(90001, "90001"))
            );
    }
}
