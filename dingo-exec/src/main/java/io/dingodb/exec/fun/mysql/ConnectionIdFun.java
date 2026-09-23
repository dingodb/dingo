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
import io.dingodb.common.session.SessionUtil;
import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Connection;
import java.util.Map;

/**
 * MySQL CONNECTION_ID() function.
 *
 * <p>Returns the connection ID (thread ID) for the current MySQL protocol
 * connection. The value is stable within one physical connection and
 * distinct across concurrent connections. It matches the identifier assigned
 * in the MySQL handshake packet, shown in processlist and used by the
 * {@code mysql:<threadId>} connection management mapping.</p>
 *
 * <p>The connection identity string is injected as operands by
 * {@code DingoDriverParser#deepSugar} at planning time, following the same
 * convention as DATABASE()/USER().</p>
 */
public class ConnectionIdFun extends BinaryOp {
    public static final String NAME = "connection_id";
    @SuppressWarnings("serial")
    private static final long serialVersionUID = -7637126409185235211L;

    public static final ConnectionIdFun INSTANCE = new ConnectionIdFun();

    private static final String MYSQL_CONNECTION_PREFIX = "mysql:";

    @Override
    public Object evalValue(Object value0, Object value1, ExprConfig config) {
        if (value0 == null) {
            return null;
        }
        String connId = value0.toString();
        SessionUtil sm = ExecutionEnvironment.INSTANCE.sessionUtil;
        for (Map.Entry<String, Connection> entry : sm.connectionMap.entrySet()) {
            Connection conn = entry.getValue();
            if (conn != null && conn.toString().equalsIgnoreCase(connId)) {
                String key = entry.getKey();
                if (key != null && key.startsWith(MYSQL_CONNECTION_PREFIX)) {
                    try {
                        return Long.parseLong(key.substring(MYSQL_CONNECTION_PREFIX.length()));
                    } catch (NumberFormatException ignore) {
                        // fall through to derived id
                    }
                }
                break;
            }
        }
        // Non-MySQL-protocol connection (e.g. embedded host driver): derive a
        // stable, per-connection distinct identifier instead of a constant.
        return (long) (connId.hashCode() & 0x7fffffffL);
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }

    @Override
    public Type getType() {
        return Types.LONG;
    }
}
