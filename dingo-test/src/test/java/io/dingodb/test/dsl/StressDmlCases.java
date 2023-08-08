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
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public final class StressDmlCases extends SqlTestCaseJavaBuilder {
    private static final int RECORD_NUM = 100;

    public StressDmlCases() {
        super("Stress");
    }

    @Override
    protected void build() {
        table("i4k_vs_f80", file("i4k_vs_f80/create.sql"));

        test("Insert")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .custom(context -> {
                Random random = new Random();
                try (Statement statement = context.getStatement()) {
                    for (int id = 1; id <= RECORD_NUM; ++id) {
                        String sql = "insert into {table} values"
                            + "(" + id + ", '" + UUID.randomUUID() + "', " + random.nextDouble() + ")";
                        log.info("Exec {}", sql);
                        statement.execute(context.transSql(sql));
                        assertThat(statement.getUpdateCount()).isEqualTo(1);
                    }
                }
            });

        test("Insert with parameters")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .custom(context -> {
                Random random = new Random();
                String sql = "insert into {table} values(?, ?, ?)";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    for (int id = 1; id <= RECORD_NUM; ++id) {
                        statement.setInt(1, id);
                        statement.setString(2, UUID.randomUUID().toString());
                        statement.setDouble(3, random.nextDouble());
                        log.info("Exec {}, id = {}", sql, id);
                        statement.execute();
                        assertThat(statement.getUpdateCount()).isEqualTo(1);
                    }
                }
            });

        test("Batch insert with parameters")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .custom(context -> {
                Random random = new Random();
                String sql = "insert into {table} values(?, ?, ?)";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    for (int id = 1; id <= RECORD_NUM; ++id) {
                        statement.setInt(1, id);
                        statement.setString(2, UUID.randomUUID().toString());
                        statement.setDouble(3, random.nextDouble());
                        statement.addBatch();
                        if (id % 100 == 0) {
                            int[] updateCounts = statement.executeBatch();
                            log.info("execute batch {}, id = {}", sql, id);
                            assertThat(updateCounts).containsOnly(1);
                            statement.clearBatch();
                        }
                    }
                }
            });
    }
}
