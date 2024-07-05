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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.session.SessionManager;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.meta.InfoSchemaService;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

@Slf4j
public class ScopeVarFun extends BinaryOp {
    private static final long serialVersionUID = 8240453507123773562L;

    public static final ScopeVarFun INSTANCE = new ScopeVarFun();

    public static final String NAME = "@@";

    @Override
    public Object evalValue(Object value0, Object value1, ExprConfig config) {
        SessionManager sm = ExecutionEnvironment.INSTANCE.sessionManager;
        Connection connection = sm.connectionMap
            .values()
            .stream()
            .filter(v -> v.toString().equalsIgnoreCase(value1.toString()))
            .findFirst().orElse(null);
        if (connection == null) {
            return "";
        }

        String variableName = value0.toString().replace("'", "").toLowerCase();
        String value = null;

        if (variableName.startsWith("global.")) {
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            Map<String, String> globalVariableMap = infoSchemaService.getGlobalVariables();

            variableName = variableName.substring(7);
            value = globalVariableMap.getOrDefault(variableName, "");
        } else if (variableName.startsWith("session.")) {
            variableName = variableName.substring(8);
            try {
                if ("tx_isolation".equals(variableName)) {
                    variableName = "transaction_isolation";
                } else if ("tx_read_only".equals(variableName)) {
                    variableName = "transaction_read_only";
                }
                value = connection.getClientInfo(variableName);
            } catch (SQLException e) {
                LogUtils.error(log, e.getMessage(), e);
            }
        } else {
            try {
                value = connection.getClientInfo(variableName);
            } catch (SQLException e) {
                LogUtils.error(log, e.getMessage(), e);
            }
        }
        if (value == null) {
            value = "";
        }
        if (value.equalsIgnoreCase("on")) {
            return "1";
        } else if (value.equalsIgnoreCase("off")) {
            return "0";
        } else {
            return value;
        }
    }
}
