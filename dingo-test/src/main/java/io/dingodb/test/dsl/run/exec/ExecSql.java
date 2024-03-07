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

package io.dingodb.test.dsl.run.exec;

import io.dingodb.test.dsl.run.check.Check;
import io.dingodb.test.dsl.run.check.CheckContext;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.SQLException;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class ExecSql implements Exec {
    private final String sqlString;
    private final Check check;

    public ExecSql(String sqlString) {
        this(sqlString, null);
    }

    @Override
    public void run(@NonNull SqlExecContext execContext) throws SQLException {
        CheckContext checkContext = execContext.execSql(sqlString);
        if (check != null) {
            check.check(checkContext);
        } else {
            if (checkContext.getException() != null) {
                throw new SQLException(checkContext.getException());
            }
        }
    }
}
