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

package io.dingodb.calcite.executor;

import org.apache.calcite.sql.SqlSetOption;

import java.sql.Connection;
import java.util.List;

public class SetOptionsExecutor implements DdlExecutor {
    List<SqlSetOption> sqlSetOptionList;
    Connection connection;

    public SetOptionsExecutor(Connection connection, List<SqlSetOption> sqlSetOptionList) {
        this.sqlSetOptionList = sqlSetOptionList;
        this.connection = connection;
    }

    @Override
    public void execute() {
        sqlSetOptionList.forEach(sqlSetOption -> {
            SetOptionExecutor setOptionExecutor = new SetOptionExecutor(connection, sqlSetOption);
            setOptionExecutor.execute();
        });
    }
}
