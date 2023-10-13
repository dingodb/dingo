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

package io.dingodb.example;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dingodb.client.DingoClient;
import io.dingodb.client.DingoOpCli;
import io.dingodb.client.common.Key;
import io.dingodb.client.common.Record;
import io.dingodb.client.common.Value;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.driver.client.DingoDriverClient;
import io.dingodb.example.model.Person;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.TableDefinition;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringJoiner;
import java.util.stream.Collectors;


public class DingoExamples {

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

    @Parameter(description = "[jdbc] or [sdk] or [schema] or [pojo], if schema, show table schema.", order = 1, required = true)
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
        DingoExamples examples = new DingoExamples();
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
            case "JDBC": // operating a table using SQL
                Class.forName("io.dingodb.driver.client.DingoDriverClient");
                runOperation(new JDBCRunner(
                    table.toUpperCase(), DriverManager.getConnection(
                        DingoDriverClient.CONNECT_STRING_PREFIX + "url=" + jdbcUrl, "root", "123123")
                ));
                break;
            case "SDK": // Operate tables using interfaces
                // Parse the configuration file to obtain the coordinator address. eg: dingo-dist/conf/client.yaml
                DingoConfiguration.parse(config);
                String coordinatorServerList = Configuration.coordinatorExchangeSvrList();
                DingoClient dingoClient = new DingoClient(coordinatorServerList, 10);
                runOperation(new SDKRunner(
                    table.toUpperCase(),
                    dingoClient)
                );
                break;
            case "SCHEMA": // show table schema
                System.out.println(TABLE_SCHEMA);
                break;
            case "POJO": // Use object classes to manipulate tables
                DingoConfiguration.parse(config);
                String coordinatorserverlist = Configuration.coordinatorExchangeSvrList();
                DingoClient dingoClient1 = new DingoClient(coordinatorserverlist, 10);
                runOperation(new OpCliRunner(
                    table.toUpperCase(),
                    dingoClient1)
                );
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + option);
        }
    }

    @SuppressWarnings("checkstyle:FallThrough")
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
                // runner.insert(records);
                break;
            }
            case "QUERY": {
                long total = 0;
                long elapsed;
                switch (queryMode.toUpperCase()) {
                    case "ALL": {
                        runner.query(sequence, count);
                        break;
                    }
                    case "LOOP": {
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
                    }
                    case "RANDOM": {
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
                            System.out.printf("Query %d use %dms, current %dms, id: %s\n", n++, total, elapsed, id);
                        }
                    }
                    default:
                        throw new IllegalStateException("Unexpected value: " + queryMode);
                }
                break;
            }
            case "COUNT": {
                runner.count();
                break;
            }
            default: {
                throw new IllegalStateException("Unexpected value: " + operation.toUpperCase());
            }
        };
    }

    /**
     * Generate table data.
     *
     * @return row of data
     */
    private Object[] generateRecord(int seed) {
        return new Object[] {
            String.valueOf(seed),
            "name-" + seed,
            Math.abs(RANDOM.nextInt()) % 99,
            seed * 0.1,
            RANDOM.nextBoolean()};
    }

    static interface Runner {

        void create() throws Exception;

        long insert(Object[] record) throws Exception;

        long insert(List<Object[]> records) throws Exception;

        void query(int sequence, int count) throws Exception;

        long query(int seed) throws Exception;

        void count() throws Exception;
    }

    static class OpCliRunner implements Runner {

        private final String tableName;
        private final DingoOpCli dingoOpCli;

        public OpCliRunner(String tableName, DingoClient dingoClient) {
            this.tableName = tableName;
            this.dingoOpCli = new DingoOpCli.Builder(dingoClient).build();
        }

        @Override
        public void create() throws Exception {
            boolean isOk = dingoOpCli.createTable(Person.class);
            System.out.println("Create table status: " + isOk);
        }

        @Override
        public long insert(Object[] record) throws Exception {
            long start = System.currentTimeMillis();
            Person person = new Person();
            person.setId((String) record[0]);
            person.setName((String) record[1]);
            person.setAge((Integer) record[2]);
            person.setIncome((Double) record[3]);
            person.setGender((Boolean) record[4]);
            dingoOpCli.save(person);
            return System.currentTimeMillis() - start;
        }

        @Override
        public long insert(List<Object[]> records) throws Exception {
            long start = System.currentTimeMillis();
            List<Person> people = new ArrayList<>();
            for (Object[] record : records) {
                Person person = new Person();
                person.setId((String) record[0]);
                person.setName((String) record[1]);
                person.setAge((Integer) record[2]);
                person.setIncome((Double) record[3]);
                person.setGender((Boolean) record[4]);
                people.add(person);
            }
            dingoOpCli.save(people.toArray());
            return System.currentTimeMillis() - start;
        }

        @Override
        public void query(int sequence, int count) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public long query(int seed) throws Exception {
            long start = System.currentTimeMillis();
            dingoOpCli.read(Person.class, new Object[]{(String.valueOf(seed))});
            return System.currentTimeMillis() - start;
        }

        @Override
        public void count() throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    static class SDKRunner implements Runner {

        private final DingoClient dingoClient;
        private final String tableName;

        SDKRunner(String tableName, DingoClient dingoClient) {
            this.tableName = tableName;
            this.dingoClient = dingoClient;
            boolean isOK = dingoClient.open();
            if (!isOK) {
                throw new RuntimeException("Open connection failed");
            }
        }

        @Override
        public void create() throws Exception {
            ColumnDefinition c1 = ColumnDefinition.builder()
                .name("u_id").type("string").primary(1).nullable(false).build();
            ColumnDefinition c2 = ColumnDefinition.builder().name("u_name").type("string").primary(-1).build();
            ColumnDefinition c3 = ColumnDefinition.builder().name("u_age").type("integer").primary(-2).build();
            ColumnDefinition c4 = ColumnDefinition.builder().name("u_income").type("double").primary(-3).build();
            ColumnDefinition c5 = ColumnDefinition.builder().name("u_gender").type("boolean").primary(-4).build();
            TableDefinition tableDefinition = TableDefinition.builder()
                .name(tableName)
                .columns(Arrays.asList(c1, c2, c3, c4, c5))
                .replica(3)
                .engine("ENG_ROCKSDB")
                .build();

            boolean createTable = dingoClient.createTable(tableDefinition);
            System.out.println("create table success: " + createTable);
        }

        @Override
        public long insert(Object[] record) throws Exception {
            long start = System.currentTimeMillis();
            Table table = dingoClient.getTableDefinition(tableName);
            dingoClient.upsert(tableName, new Record(table.getColumns(), record));
            return System.currentTimeMillis() - start;
        }

        @Override
        public long insert(List<Object[]> records) throws Exception {
            long start = System.currentTimeMillis();
            Table table = dingoClient.getTableDefinition(tableName);
            dingoClient.upsert(
                tableName,
                records.stream().map(record -> new Record(table.getColumns(), record)).collect(Collectors.toList()));
            return System.currentTimeMillis() - start;
        }

        @Override
        public void query(int sequence, int count) throws Exception {
            Key startKey = new Key(Value.get(String.valueOf(sequence)));
            Key endKey = new Key(Value.get(String.valueOf(count)));
            Iterator<Record> iterator = dingoClient.scan(tableName, startKey, endKey, true, true);
            int line = 0;
            while (iterator.hasNext()) {
                List<Object> objects = iterator.next().getColumnValuesInOrder();
                System.out.printf(
                    "Query result u_id=%s, u_name=%s, u_age=%s, u_income=%s, u_gender=%s. \n",
                    objects.get(0), objects.get(1), objects.get(2), objects.get(3), objects.get(4)
                );
                line++;
            }
            System.out.println("Count: " + line);
        }

        @Override
        public long query(int index) throws Exception {
            long start = System.currentTimeMillis();
            List<Object> objects = dingoClient.get(tableName, new Key(Value.get(String.valueOf(index))))
                .getColumnValuesInOrder();
            System.out.printf(
                "Query result u_id=%s, u_name=%s, u_age=%s, u_income=%s, u_gender=%s. \n",
                objects.get(0), objects.get(1), objects.get(2), objects.get(3), objects.get(4)
            );
            return System.currentTimeMillis() - start;
        }

        @Override
        public void count() throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    static class JDBCRunner implements Runner {

        private final String tableName;
        private final Connection connection;

        JDBCRunner(String tableName, Connection connection) {
            this.tableName = tableName;
            this.connection = connection;
        }

        @Override
        public void create() throws Exception {
            try (Statement statement = connection.createStatement()) {
                StringBuilder sqlBuilder = new StringBuilder()
                    .append("create table ").append(tableName).append(" (\n")
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
                sqlBuilder.append("insert into ").append(tableName).append(" values");
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
        public void query(int sequence, int count) throws Exception {
            String sql = "select * from " + tableName;
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    int line = 0;
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
        public long query(int index) throws Exception {
            String sql = "select * from " + tableName + " where u_id = '" + index + "'";
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
            String sql = "select count(*) cnt from " + tableName;
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery(sql);
                resultSet.next();
                System.out.println("Count: " + resultSet.getInt("cnt"));
            }
            System.out.printf("Query use %sms\n", System.currentTimeMillis() - start);
        }
    }

}
