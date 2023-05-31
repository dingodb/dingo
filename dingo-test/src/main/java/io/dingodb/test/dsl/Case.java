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

import io.dingodb.test.asserts.Assert;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.provider.Arguments;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Case {
    private final boolean enabled;
    private final List<Step> steps;

    public static @NonNull Case of(Step... steps) {
        return new Case(true, Arrays.asList(steps));
    }

    public static @NonNull Arguments of(@NonNull String name, Step... steps) {
        boolean enabled = !name.startsWith("[skip]");
        return Arguments.arguments(name, new Case(enabled, Arrays.asList(steps)));
    }

    public static @NonNull Step exec(String sqlString) {
        return new Exec(sqlString, null, null);
    }

    public static @NonNull Step exec(InputStream inputStream) {
        return new ExecFile(inputStream, null, null);
    }

    public static @NonNull InputStream file(String fileName) {
        try {
            throw new Exception();
        } catch (Exception exception) {
            String className = exception.getStackTrace()[1].getClassName();
            try {
                InputStream is = Class.forName(className).getResourceAsStream(fileName);
                if (is != null) {
                    return is;
                }
                throw new FileNotFoundException("Cannot access file \"" + fileName
                    + "\" in resources of class \"" + className + "\".");
            } catch (ClassNotFoundException | FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static String transSql(String sqlString, RandomTable @NonNull ... tables) {
        for (RandomTable table : tables) {
            sqlString = table.transSql(sqlString);
        }
        return sqlString;
    }

    private static RandomTable @NonNull [] initRandomTable(int num) {
        RandomTable[] randomTables = new RandomTable[num];
        for (int i = 0; i < num; ++i) {
            randomTables[i] = new RandomTable();
        }
        return randomTables;
    }

    public static void dropRandomTables(
        Connection connection,
        RandomTable @NonNull ... randomTables
    ) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (RandomTable randomTable : randomTables) {
                statement.execute("drop table " + randomTable.getName());
            }
        }
    }

    public void run(Connection connection, RandomTable... randomTables) throws Exception {
        assumeTrue(enabled);
        try (Statement statement = connection.createStatement()) {
            for (Step step : steps) {
                step.run(statement, randomTables);
            }
        }
    }

    public void run(Connection connection) throws Exception {
        run(connection, 1);
    }

    public void run(Connection connection, int randomTableNum) throws Exception {
        RandomTable[] randomTables = initRandomTable(randomTableNum);
        run(connection, randomTables);
        dropRandomTables(connection, randomTables);
    }

    public void runWithStatementForEachStep(Connection connection) throws Exception {
        runWithStatementForEachStep(connection, 1);
    }

    public void runWithStatementForEachStep(Connection connection, int randomTableNum) throws Exception {
        assumeTrue(enabled);
        RandomTable[] randomTables = initRandomTable(randomTableNum);
        for (Step step : steps) {
            try (Statement statement = connection.createStatement()) {
                step.run(statement, randomTables);
            }
        }
        dropRandomTables(connection, randomTables);
    }

    public interface Step {
        void run(@NonNull Statement statement, RandomTable... tables) throws Exception;

        @NonNull Step result(InputStream resultFile);

        @NonNull Step result(String... csvString);

        @NonNull Step result(String[] columnLabels, List<Object[]> tuples);

        @NonNull Step updateCount(int updateCount);
    }

    public interface ResultChecker {
        void assertSame(ResultSet resultSet) throws Exception;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class Exec implements Step {
        private final String sqlString;
        private final ResultChecker resultChecker;
        private final Integer updateCount;

        @Override
        public void run(@NonNull Statement statement, RandomTable... tables) throws Exception {
            Boolean b = null;
            for (String s : sqlString.split(";")) {
                if (!s.trim().isEmpty()) {
                    b = statement.execute(transSql(s, tables));
                }
            }
            if (resultChecker != null) {
                assertThat(b).isTrue();
                try (ResultSet resultSet = statement.getResultSet()) {
                    resultChecker.assertSame(resultSet);
                }
            } else if (updateCount != null) {
                assertThat(b).isFalse();
                assertThat(statement.getUpdateCount()).isEqualTo(updateCount);
            }
        }

        @Override
        public @NonNull Step result(InputStream resultFile) {
            return new Exec(sqlString, new CsvFileResultChecker(resultFile), null);
        }

        @Override
        public @NonNull Step result(String... csvString) {
            return new Exec(
                sqlString,
                new CsvResultChecker(String.join("\n", csvString)),
                null
            );
        }

        @Override
        public @NonNull Step result(String[] columnLabels, List<Object[]> tuples) {
            return new Exec(
                sqlString,
                new ColumnResultChecker(columnLabels, tuples),
                null
            );
        }

        @Override
        public @NonNull Step updateCount(int updateCount) {
            return new Exec(sqlString, null, updateCount);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class ExecFile implements Step {
        private final InputStream inputStream;
        private final ResultChecker resultChecker;
        private final Integer updateCount;

        @Override
        public void run(@NonNull Statement statement, RandomTable... tables) throws Exception {
            String sqlString = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            new Exec(sqlString, resultChecker, updateCount).run(statement, tables);
        }

        @Override
        public @NonNull Step result(InputStream resultFile) {
            return new ExecFile(inputStream, new CsvFileResultChecker(resultFile), null);
        }

        @Override
        public @NonNull Step result(String... csvString) {
            return new ExecFile(
                inputStream,
                new CsvResultChecker(String.join("\n", csvString)),
                null
            );
        }

        @Override
        public @NonNull Step result(String[] columnLabels, List<Object[]> tuples) {
            return new ExecFile(
                inputStream,
                new ColumnResultChecker(columnLabels, tuples),
                null
            );
        }

        @Override
        public @NonNull Step updateCount(int updateCount) {
            return new ExecFile(inputStream, null, updateCount);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class CsvFileResultChecker implements ResultChecker {
        private final InputStream inputStream;

        @Override
        public void assertSame(ResultSet resultSet) throws Exception {
            Assert.resultSet(resultSet).asInCsv(inputStream);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class CsvResultChecker implements ResultChecker {
        private final String csvString;

        @Override
        public void assertSame(ResultSet resultSet) throws Exception {
            InputStream inputStream = new ReaderInputStream(new StringReader(csvString), StandardCharsets.UTF_8);
            new CsvFileResultChecker(inputStream).assertSame(resultSet);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class ColumnResultChecker implements ResultChecker {
        private final String[] columnLabels;
        private final List<Object[]> tuples;

        @Override
        public void assertSame(ResultSet resultSet) throws Exception {
            Assert.resultSet(resultSet)
                .columnLabels(columnLabels)
                .isRecords(tuples);
        }
    }
}
