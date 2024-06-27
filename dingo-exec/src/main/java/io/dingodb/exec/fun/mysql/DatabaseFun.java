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

package io.dingodb.exec.fun.mysql;

import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.session.SessionManager;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseFun extends BinaryOp {
    private static final long serialVersionUID = -8922458990894302550L;

    public static final DatabaseFun INSTANCE = new DatabaseFun();

    public static final String NAME = "database";

    @Override
    public Object evalValue(Object value0, Object value1, ExprConfig config) {
        SessionManager sm = ExecutionEnvironment.INSTANCE.sessionManager;;
        Connection connection = sm.connectionMap
            .values()
            .stream()
            .filter(v -> v.toString().equalsIgnoreCase(value1.toString()))
            .findFirst().orElse(null);
        try {
            if (connection != null) {
                return connection.getSchema();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return value0;
    }
}
