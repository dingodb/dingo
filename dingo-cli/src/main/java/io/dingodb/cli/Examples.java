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

package io.dingodb.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.sdk.client.DingoClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.StringJoiner;
import javax.annotation.Nonnull;

import static io.dingodb.driver.client.DingoDriverClient.CONNECT_STRING_PREFIX;

public class Examples {

    public static final String TABLE_SCHEMA = "Table schema: \n"
        + "| -------------- | ---------------------- |\n"
        + "| ColumnName     | ColumnType             |\n"
        + "| -------------- | ---------------------- |\n"
        + "| u_id           | varchar(32) not null   |\n"
        + "| u_name         | varchar(32)            |\n"
        + "| u_age          | int                    |\n"
        + "| u_income       | double                 |\n"
        + "| u_gender       | boolean                |\n"
        + "| -------------- | ---------------------- |\n";

    private static final Random RANDOM = new Random();

    @Parameter(names = "--help", help = true, order = 0)
    private boolean help;

    @Parameter(description = "[driver] or [sdk] or [schema], if schema, show table schema.", order = 1, required = true)
    private String option;

    @Parameter(names = "--config", description = "Config file path.", order = 2)
    private String config;

    @Parameter(names = "--op", description = "Operation, such as [create], [insert].", order = 3)
    private String operation;

    @Parameter(names = "--table", description = "Table name.", order = 4)
    private String table;

    @Parameter(names = "--sequence", description = "Insert data start sequence, default 1.", order = 5)
    private Integer sequence = 1;

    @Parameter(
        names = "--count",
        description = "Fake data count, default 10000, if error, the actual count is not accurate",
        order = 6
    )
    private Integer count = 10000;

    @Parameter(
        names = "--batch",
        description = "Fake data batch, default 1000, if error, the actual count is not accurate",
        order = 7
    )
    private Integer batch = 1000;

    @Parameter(
        names = "--url",
        description = "JDBC url.",
        order = 8
    )
    private String jdbcUrl;

    @Parameter(
        names = "--query",
        description = "Query [all] on one sql, [loop] from sequence to count, [random] from sequence to count.",
        order = 8
    )
    private String queryMode;


    public static void main(String[] args) throws Exception {
        Examples examples = new Examples();
        JCommander commander = new JCommander(examples);
        commander.parse(args);
        examples.exec(commander);
    }

    public void exec(JCommander commander) throws Exception {
        if (help) {
            commander.usage();
            return;
        }

        switch (option.toUpperCase()) {
            case "JDBC":
                Class.forName("io.dingodb.driver.client.DingoDriverClient");
                runOperation(new JDBCRunner(
                    table.toUpperCase(),
                    DriverManager.getConnection(CONNECT_STRING_PREFIX + "url=" + jdbcUrl)
                ));
                break;
            case "SDK":
                runOperation(new SDKRunner(new DingoClient(config, table.toUpperCase())));
                break;
            case "SCHEMA":
                System.out.println(TABLE_SCHEMA);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + option);
        }
    }

    public void runOperation(Runner runner) throws Exception {
        switch (operation.toUpperCase()) {
            case "CREATE": {
                runner.create();
                break;
            }
            case "INSERT": {
                long elapsed;
                long total = 0;
                for (int i = sequence; i <= count; i++) {
                    elapsed = runner.insert(generateRecord(i));
                    System.out.printf("Insert %d use %dms, current %dms\n", i, total += elapsed, elapsed);
                }
                break;
            }
            case "BATCH": {
                List<Object[]> records = new ArrayList<>();
                long total = 0;
                long elapsed;
                for (int i = sequence; i <= count; i++) {
                    records.add(generateRecord(i));
                    if (records.size() >= batch) {
                        elapsed = runner.insert(records);
                        records.clear();
                        System.out.printf("Insert %d use %dms, current %dms\n", i, total += elapsed, elapsed);
                    }
                }
                runner.insert(records);
                break;
            }
            case "QUERY":
                long total = 0;
                long elapsed;
                switch (queryMode.toUpperCase()) {
                    case "ALL":
                        runner.query();
                        break;
                    case "LOOP":
                        for (int i = sequence; i <= count; i++) {
                            try {
                                total += elapsed = runner.query(i);
                            } catch (Exception e) {
                                System.out.printf("Query [%d] error\n", i);
                                throw e;
                            }
                            if (elapsed == -1) {
                                return;
                            }
                            System.out.printf("Query %d use %dms, current %dms\n", i - sequence + 1, total, elapsed);
                        }
                        break;
                    case "RANDOM":
                        long n = 1;
                        while (true) {
                            int id = Math.abs(RANDOM.nextInt() % (count - sequence)) + sequence;
                            try {
                                total += elapsed = runner.query(id);
                            } catch (Exception e) {
                                System.out.printf("Query [%d] error\n", id);
                                throw e;
                            }
                            if (elapsed == -1) {
                                return;
                            }
                            System.out.printf("Query %d use %dms, current %dms, id: %s\n", n++ , total, elapsed, id);
                        }
                    default:
                        throw new IllegalStateException("Unexpected value: " + queryMode);
                }
                break;
            case "COUNT": {
                runner.count();
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + operation.toUpperCase());
        };
    }

    @Nonnull
    private Object[] generateRecord(int n) {
        return new Object[] {
            String.valueOf(n),
            "name-" + n,
            Math.abs(RANDOM.nextInt()) % 99,
            n * 0.1,
            RANDOM.nextBoolean()};
    }

    static interface Runner {

        void create() throws Exception;

        long insert(Object[] record) throws Exception;

        long insert(List<Object[]> records) throws Exception;

        void query() throws Exception;

        long query(int i) throws Exception;

        void count() throws Exception;
    }

    static class SDKRunner implements Runner {

        private final DingoClient dingoClient;

        SDKRunner(DingoClient dingoClient) {
            this.dingoClient = dingoClient;
        }

        @Override
        public void create() throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public long insert(Object[] record) throws Exception {
            long start = System.currentTimeMillis();
            dingoClient.insert(record);
            return System.currentTimeMillis() - start;
        }

        @Override
        public long insert(List<Object[]> records) throws Exception {
            long start = System.currentTimeMillis();
            dingoClient.insert(records);
            return System.currentTimeMillis() - start;
        }

        @Override
        public void query() throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public long query(int n) throws Exception {
            long start = System.currentTimeMillis();
            Object[] objects = dingoClient.get(new Object[] {String.valueOf(n)});
            System.out.printf(
                "Query result u_id=%s, u_name=%s, u_age=%s, u_income=%s, u_gender=%s. \n",
                objects[0], objects[1], objects[2], objects[3], objects[4]
            );
            return System.currentTimeMillis() - start;
        }

        @Override
        public void count() throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    static class JDBCRunner implements Runner {

        private final String table;
        private final Connection connection;

        JDBCRunner(String table, Connection connection) {
            this.table = table;
            this.connection = connection;
        }

        @Override
        public void create() throws Exception {
            try (Statement statement = connection.createStatement()) {
                StringBuilder sqlBuilder = new StringBuilder()
                    .append("create table ").append(table).append(" (\n")
                    .append("u_id       varchar(32) not null,\n")
                    .append("u_name     varchar(32),\n")
                    .append("u_age      int,\n")
                    .append("u_income   double,\n")
                    .append("u_gender   boolean,\n")
                    .append("primary key(u_id)\n")
                    .append(")");
                long start = System.currentTimeMillis();
                statement.execute(sqlBuilder.toString());
                System.currentTimeMillis();
            }
        }

        @Override
        public long insert(Object[] record) throws Exception {
            return insert(Collections.singletonList(record));
        }

        @Override
        public long insert(List<Object[]> records) throws Exception {
            try (Statement statement = connection.createStatement()) {
                StringBuilder sqlBuilder = new StringBuilder();
                sqlBuilder.append("insert into ").append(table).append(" values");
                StringJoiner joiner = new StringJoiner(",").setEmptyValue("");
                for (Object[] record : records) {
                    joiner
                        .add("'" + record[0].toString() + "'")
                        .add("'" + record[1].toString() + "'")
                        .add(record[2].toString())
                        .add(record[3].toString())
                        .add(record[4].toString());
                    sqlBuilder.append("(").append(joiner).append("),");
                    joiner = new StringJoiner(",").setEmptyValue("");
                }
                long start = System.currentTimeMillis();
                statement.execute(sqlBuilder.substring(0, sqlBuilder.length() - 1));
                return System.currentTimeMillis() - start;
            }
        }

        @Override
        public void query() throws Exception {
            String sql = "select * from " + table;
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    int line = 1;
                    StringJoiner joiner = new StringJoiner(",").setEmptyValue("");
                    while (resultSet.next()) {
                        for (int i = 1; i < columnCount + 1; i++) {
                            if (resultSet.getObject(i) == null) {
                                joiner.add(null);
                            } else {
                                joiner.add(resultSet.getString(i));
                            }
                        }
                        System.out.println(joiner);
                        joiner = new StringJoiner(",").setEmptyValue("");
                        line++;
                    }
                    System.out.println("Count: " + line);
                }
            }
        }

        @Override
        public long query(int i) throws Exception {
            String sql = "select * from " + table + " where u_id = '" + i + "'";
            long time = System.currentTimeMillis();
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    if (!resultSet.next()) {
                        return -1;
                    }
                    System.out.printf(
                        "Query result u_id=%s, u_name=%s, u_age=%s, u_income=%s, u_gender=%s. \n",
                        resultSet.getString("u_id"),
                        resultSet.getString("u_name"),
                        resultSet.getInt("u_age"),
                        resultSet.getDouble("u_income"),
                        resultSet.getBoolean("u_gender")
                    );
                }
            } catch (Exception e) {
                e.printStackTrace();
                return -1;
            }
            return System.currentTimeMillis() - time;
        }

        @Override
        public void count() throws Exception {
            long start = System.currentTimeMillis();
            String sql = "select count(*) cnt from " + table;
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery(sql);
                resultSet.next();
                System.out.println("Count: " + resultSet.getInt("cnt"));
            }
            System.out.printf("Query use %sms\n", System.currentTimeMillis() - start);
        }
    }

}
