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

package io.dingodb.test.dsl.builder.checker;

import io.dingodb.test.dsl.run.check.CheckContext;
import io.dingodb.test.dsl.run.check.SqlCheckerConverter;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Used for checking directly in custom steps.
 */
@RequiredArgsConstructor
public class CheckRunningContext {
    @Getter
    private final Statement statement;
    @Getter
    private final Boolean executeReturnedValue;
    @Getter
    private final String info;

    private void run(SqlChecker checker) throws SQLException {
        SqlCheckerConverter.of(null, null).visit(checker).check(
            new CheckContext(statement, executeReturnedValue, info));
    }

    public void csv(String... csvLines) throws SQLException {
        run(new SqlCsvStringResultChecker(csvLines));
    }

    public void csv(InputStream csvFile) throws SQLException {
        run(new SqlCsvFileResultChecker(csvFile));
    }

    public void is(String[] columnLabels, List<Object[]> tuples) throws SQLException {
        run(new SqlObjectResultChecker(columnLabels, tuples));
    }

    public void count(int updateCount) throws SQLException {
        run(new SqlUpdateCountChecker(updateCount));
    }
}
