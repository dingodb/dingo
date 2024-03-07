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

import java.sql.PreparedStatement;

public class ParameterQueryCases extends SqlTestCaseJavaBuilder {
    protected ParameterQueryCases() {
        super("Parameter");
    }

    @Override
    protected void build() {
        test("select 1 + ?")
            .custom(context -> {
                String sql = "select 1 + ?";
                try (PreparedStatement statement = context.getConnection().prepareStatement(sql)) {
                    statement.setInt(1, 1);
                    boolean b = statement.execute();
                    check(statement, b, sql).test(is(
                        new String[]{"EXPR$0"},
                        ImmutableList.of(
                            new Object[]{2}
                        )));
                    statement.setInt(1, 2);
                    b = statement.execute();
                    check(statement, b, sql).test(is(
                        new String[]{"EXPR$0"},
                        ImmutableList.of(
                            new Object[]{3}
                        )));
                }
            });

        table("i4k_vs_f80", file("cases/tables/i4k_vs_f80.create.sql"))
            .init(file("cases/tables/i4k_vs_f80.data.sql"), 9);

        test("Select filtered")
            .use("table", "i4k_vs_f80")
            .custom(context -> {
                String sql = "select * from {table} where id < ? and amount > ?";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    statement.setInt(1, 8);
                    statement.setDouble(2, 5.0);
                    boolean b = statement.execute();
                    check(statement, b, sql).test(is(
                        new String[]{"ID", "NAME", "AMOUNT"},
                        ImmutableList.of(
                            new Object[]{5, "Emily", 5.5},
                            new Object[]{6, "Alice", 6.0},
                            new Object[]{7, "Betty", 6.5}
                        )
                    ));
                    statement.setDouble(2, 6.0);
                    b = statement.execute();
                    check(statement, b, sql).test(is(
                        new String[]{"ID", "NAME", "AMOUNT"},
                        ImmutableList.of(
                            new Object[]{7, "Betty", 6.5}
                        )
                    ));
                }
            });

        test("Select by primary key")
            .use("table", "i4k_vs_f80")
            .custom(context -> {
                String sql = "select * from {table} where id = ?";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    statement.setInt(1, 1);
                    boolean b = statement.execute();
                    check(statement, b, sql).test(is(
                        new String[]{"ID", "NAME", "AMOUNT"},
                        ImmutableList.of(
                            new Object[]{1, "Alice", 3.5}
                        )
                    ));
                    statement.setInt(1, 2);
                    b = statement.execute();
                    check(statement, b, sql).test(is(
                        new String[]{"ID", "NAME", "AMOUNT"},
                        ImmutableList.of(
                            new Object[]{2, "Betty", 4.0}
                        )
                    ));
                }
            });

        test("Select filtered with fun")
            .use("table", "i4k_vs_f80")
            .custom(context -> {
                String sql = "select * from {table} where locate(?, name) <> 0";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    statement.setString(1, "i");
                    boolean b = statement.execute();
                    check(statement, b, sql).test(is(
                        new String[]{"ID", "NAME", "AMOUNT"},
                        ImmutableList.of(
                            new Object[]{1, "Alice", 3.5},
                            new Object[]{3, "Cindy", 4.5},
                            new Object[]{4, "Doris", 5.0},
                            new Object[]{5, "Emily", 5.5},
                            new Object[]{6, "Alice", 6.0},
                            new Object[]{8, "Alice", 7.0},
                            new Object[]{9, "Cindy", 7.5}
                        )
                    ));
                    statement.setString(1, "o");
                    b = statement.execute();
                    check(statement, b, sql).test(is(
                        new String[]{"ID", "NAME", "AMOUNT"},
                        ImmutableList.of(
                            new Object[]{4, "Doris", 5.0}
                        )
                    ));
                }
            });
    }
}
