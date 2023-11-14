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

import static io.dingodb.common.exception.DingoSqlException.CUSTOM_ERROR_STATE;
import static io.dingodb.common.exception.DingoSqlException.TEST_ERROR_CODE;
import static io.dingodb.common.exception.DingoSqlException.UNKNOWN_ERROR_CODE;
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
                exception(sql(UNKNOWN_ERROR_CODE, CUSTOM_ERROR_STATE))
            );

        // DDL error
        test("SQL Parse error")
            .step("select", exception(sql(1064, "42000")));

        test("Illegal expression in context")
            .step("insert into {table} (1)", exception(sql(2002, CUSTOM_ERROR_STATE)));

        test("Unknown identifier")
            .step(
                "create table {table} (id int, data bomb, primary key(id))",
                exception(sql(2003, CUSTOM_ERROR_STATE))
            );

        test("Table already exists")
            .use("table", "i4k_vs_i40_f80_vs0_l0")
            .step(
                "create table {table} (id int, primary key(id))",
                exception(sql(1050, "42S01"))
            );

        test("Non exist primary key")
            .step(
                "create table test2(uuid varchar, phone varchar, birthday date, primary key(id))",
                exception(sql(1002, CUSTOM_ERROR_STATE))
            );

        test("Missing column list")
            .step("create table {table}", exception(sql(4028, "HY000")));

        test("Table not found")
            .step("select * from {table}", exception(sql(1146, "42S02")));

        test("Column not found")
            .use("table", "i4k_vs_i40_f80_vs0_l0")
            .step("select not_exist from {table}", exception(sql(1054, "42S22")));

        test("Column not allow null")
            .step("create table {table} (id int, primary key(id))")
            .step("insert into {table} values(null)", exception(sql(1364, "HY000")));

        test("Column not allow null (array)")
            .step("create table {table} (id int, data varchar array not null, primary key(id))")
            .step("insert into {table} values(1, null)", exception(sql(1364, "HY000")));

        test("Column not allow null (map)")
            .step("create table {table} (id int, data map not null, primary key(id))")
            .step("insert into {table} values(1, null)", exception(sql(1364, "HY000")));

        test("Number format error")
            .step("create table {table} (id int, data int, primary key(id))")
            .step("insert into {table} values(1, 'abc')", exception(sql(3001, CUSTOM_ERROR_STATE)));

        test("Illegal use of dynamic parameter")
            .custom((context) -> {
                String sql = "select ?";
                SQLException e = assertThrows(SQLException.class, () -> {
                    try (PreparedStatement statement = context.getConnection().prepareStatement(sql)) {
                        statement.execute();
                    }
                });
                assertThat(e.getErrorCode()).isEqualTo(2001);
                assertThat(e.getSQLState()).isEqualTo(CUSTOM_ERROR_STATE);
            });

        // Execution error
        test("Task failed")
            .step("create table {table} (id int, data double, primary key(id))")
            .step("insert into {table} values (1, 3.5)", count(1))
            .step("update {table} set data = 'abc'", exception(sql(5001, CUSTOM_ERROR_STATE)));

        // Unknown
        test("Cast float to int")
            .step(
                "select cast(18293824503.55 as int) ca",
                exception(sql(UNKNOWN_ERROR_CODE, CUSTOM_ERROR_STATE))
            );

        // Intentionally
        test("By `thrown` function")
            .step("select throw()", exception(sql(TEST_ERROR_CODE, CUSTOM_ERROR_STATE)));

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
                exception(sql(UNKNOWN_ERROR_CODE, CUSTOM_ERROR_STATE))
            );

        test("Null in array")
            .step("create table {table} (id int, data varchar array, primary key(id))")
            .step(
                "insert into {table} values(1, array['1', null, '3'])",
                exception(sql(UNKNOWN_ERROR_CODE, CUSTOM_ERROR_STATE), "Null values are not allowed")
            );

        test("Null in multiset")
            .step("create table {table} (id int, data date multiset, primary key(id))")
            .step("insert into {table} values(1, multiset[''])",
                exception(sql(UNKNOWN_ERROR_CODE, CUSTOM_ERROR_STATE), "Null values are not allowed")
            );

        test("Null in map")
            .step("create table {table} (id int, data map, primary key(id))")
            .step(
                "insert into {table} values(1, map['a', '1', 'b', null])",
                exception(sql(UNKNOWN_ERROR_CODE, CUSTOM_ERROR_STATE), "Null values are not allowed")
            );
    }
}
