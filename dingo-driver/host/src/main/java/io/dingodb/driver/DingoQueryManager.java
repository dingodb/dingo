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

package io.dingodb.driver;

import com.google.auto.service.AutoService;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.common.ProcessInfo;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.tool.api.QueryManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class DingoQueryManager implements QueryManager {

    public static final DingoQueryManager DEFAULT_INSTANCE = new DingoQueryManager();

    @Override
    public List<ProcessInfo> getProcessInfoList() {
        return SessionUtil.INSTANCE.connectionMap
            .entrySet()
            .stream()
            .map(entry -> {
                String type = "DINGO";
                String id = entry.getKey();
                if (id.startsWith("mysql:")) {
                    type = "MYSQL";
                    id = id.substring(6);
                }
                DingoConnection dingoConn = (DingoConnection) entry.getValue();
                String txnIdStr = "";
                if (dingoConn.getTransaction() != null && dingoConn.getTransaction().getTxnId() != null) {
                    txnIdStr = dingoConn.getTransaction().getTxnId().toString();
                }
                long commandStartTime = dingoConn.getCommandStartTime();
                String costTimeStr = "0";
                String command = "query";
                if (commandStartTime == 0) {
                    command = "sleep";
                } else {
                    costTimeStr = String.valueOf(System.currentTimeMillis() - commandStartTime);
                }
                DingoParserContext context = dingoConn.getContext();
                ProcessInfo processInfo = new ProcessInfo();
                processInfo.setId(id);
                processInfo.setUser(context.getOption("user"));
                processInfo.setHost(context.getOption("host"));
                processInfo.setClient(context.getOption("client"));
                processInfo.setDb(context.getUsedSchema().getName());
                processInfo.setType(type);
                processInfo.setCommand(command);
                processInfo.setTime(costTimeStr);
                processInfo.setTxnIdStr(txnIdStr);
                try {
                    processInfo.setState(dingoConn.isClosed() ? "closed" : "open");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                String info = dingoConn.getCommand();
                processInfo.setInfo(info);
                processInfo.setSqlId(dingoConn.getQueryId());
                return processInfo;
            })
            .collect(Collectors.toList());
    }

    @Override
    public Connection getConnection(String connId) {
        return SessionUtil.INSTANCE.connectionMap.get(connId);
    }

    @Override
    public boolean hasConnection(String connId) {
        return SessionUtil.INSTANCE.connectionMap.containsKey(connId);
    }

    @AutoService(io.dingodb.tool.api.QueryManagerProvider.class)
    public static final class QueryManagerProvider
        implements io.dingodb.tool.api.QueryManagerProvider {

        @Override
        public QueryManager get() {
            return DEFAULT_INSTANCE;
        }
    }
}
