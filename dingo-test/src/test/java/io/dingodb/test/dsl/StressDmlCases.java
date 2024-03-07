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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public final class StressDmlCases extends SqlTestCaseJavaBuilder {
    public StressDmlCases() {
        super("Stress");
    }

    @Override
    protected void build() {
        table("i4k_vs_f80", file("cases/tables/i4k_vs_f80.create.sql"));

        table(
            "i4k_vs0_i40_i80_f40_f80_vs0_dt0_tm0_ts0_vs0_l0",
            file("cases/tables/i4k_vs0_i40_i80_f40_f80_vs0_dt0_tm0_ts0_vs0_l0.partitions.create.sql")
        );

        test("Insert")
            .use("table", "i4k_vs_f80")
            .modify("i4k_vs_f80")
            .custom(context -> {
                Random random = new Random();
                try (Statement statement = context.getStatement()) {
                    for (int id = 1; id <= 100; ++id) {
                        String sql = "insert into {table} values"
                            + "(" + id + ", '" + UUID.randomUUID() + "', " + random.nextDouble() + ")";
                        log.info("Exec \"{}\"", sql);
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
                    for (int id = 1; id <= 100; ++id) {
                        statement.setInt(1, id);
                        statement.setString(2, UUID.randomUUID().toString());
                        statement.setDouble(3, random.nextDouble());
                        log.info("Exec \"{}\", id = {}", sql, id);
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
                    for (int id = 1; id <= 5000; ++id) {
                        statement.setInt(1, id);
                        statement.setString(2, UUID.randomUUID().toString());
                        statement.setDouble(3, random.nextDouble());
                        statement.addBatch();
                        if (id % 500 == 0) {
                            int[] updateCounts = statement.executeBatch();
                            log.info("Execute batch \"{}\", id = {}", sql, id);
                            assertThat(updateCounts).containsOnly(1);
                            statement.clearBatch();
                        }
                    }
                }
            });

        test("Batch insert with more parameters")
            .use("table", "i4k_vs0_i40_i80_f40_f80_vs0_dt0_tm0_ts0_vs0_l0")
            .modify("i4k_vs0_i40_i80_f40_f80_vs0_dt0_tm0_ts0_vs0_l0")
            .custom(context -> {
                Random random = new Random();
                String sql = "insert into {table} values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                try (PreparedStatement statement = context.getConnection().prepareStatement(context.transSql(sql))) {
                    for (int id = 1; id <= 5000; ++id) {
                        statement.setObject(1, id);
                        String randStr = UUID.randomUUID().toString();
                        statement.setObject(2, randStr);
                        int randInt = random.nextInt();
                        statement.setObject(3, randInt);
                        long randLong = random.nextLong();
                        statement.setObject(4, randLong);
                        float randFloat = random.nextFloat();
                        statement.setObject(5, randFloat);
                        double randDouble = random.nextDouble();
                        statement.setObject(6, randDouble);
                        String randAddress = UUID.randomUUID().toString();
                        statement.setObject(7, randAddress);
                        Date randDate = new Date(0);
                        statement.setObject(8, randDate);
                        Time randTime = new Time(0);
                        statement.setObject(9, randTime);
                        Timestamp randTimestamp = new Timestamp(0);
                        statement.setObject(10, randTimestamp);
                        String randZip = UUID.randomUUID().toString();
                        statement.setObject(11, randZip);
                        statement.setObject(12, random.nextBoolean());
                        statement.addBatch();
                        if (id % 500 == 0) {
                            int[] updateCounts = statement.executeBatch();
                            log.info("Execute batch \"{}\", id = {}", sql, id);
                            assertThat(updateCounts).containsOnly(1);
                            statement.clearBatch();
                        }
                    }
                }
            });
    }
}
