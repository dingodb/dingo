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

package io.dingodb.coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.calcite.Connections;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.MessageListenerProvider;
import io.dingodb.net.SimpleMessage;
import io.dingodb.net.SimpleTag;
import io.dingodb.net.Tag;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.dingodb.expr.json.runtime.Parser.JSON;
import static java.nio.charset.StandardCharsets.UTF_8;

@Deprecated
@Slf4j
public class SqlExecutor implements MessageListener {

    public static final Tag SQL_EXECUTOR_TAG = SimpleTag.builder().tag("SQL_EXECUTOR_TAG".getBytes(UTF_8)).build();
    public static final String SELECT = "SELECT";
    public static final String SQL_SEP = "::";

    @Override
    public void onMessage(Message message, Channel channel) {
        String[] msg = new String(message.toBytes()).split(SQL_SEP);
        String type = msg[0];
        String sql = msg[1];
        if (SELECT.equalsIgnoreCase(type)) {
            try (Connection connection = Connections.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet resultSet = statement.executeQuery(sql)) {
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        List<Map<String, String>> result = new ArrayList<>();
                        int columnCount = metaData.getColumnCount();
                        int line = 1;
                        while (resultSet.next()) {
                            Map<String, String> lineMap = new HashMap<>();
                            for (int i = 1; i < columnCount + 1; i++) {
                                try {
                                    log.info("Exec sql [{}]: line=[{}], column [{}]=[{}].", sql, line,
                                        metaData.getColumnName(i), resultSet.getString(i));
                                    lineMap.put(metaData.getColumnName(i), resultSet.getString(i));
                                } catch (SQLException e) {
                                    log.error("Get sql result column error, column index: [{}].", i + 1, e);
                                }
                            }
                            line++;
                            result.add(lineMap);
                        }
                        sendResult(channel, result);
                    }
                }
            } catch (Exception e) {
                log.error("Exec sql [{}] error.", sql, e);
                try {
                    sendResult(channel, Collections.singletonMap("error", e.getMessage()));
                } catch (JsonProcessingException ex) {
                    log.error("Send error msg.", e);
                }
            }
        } else {
            try (Connection connection = Connections.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    Integer result = statement.executeUpdate(sql);
                    log.info("Exec sql [{}] [{}].", sql, result);
                    sendResult(channel, Collections.singletonMap("success lines", result));
                }
            } catch (Exception e) {
                log.error("Exec sql [{}] error.", sql, e);
                try {
                    sendResult(channel, Collections.singletonMap("error", e.getMessage()));
                } catch (JsonProcessingException ex) {
                    log.error("Send error msg.", e);
                }
            }
        }
        try {
            channel.close();
        } catch (Exception e) {
            log.error("Close channel error.", e);
        }
    }

    private void sendResult(Channel channel, Object result) throws JsonProcessingException {
        channel.send(SimpleMessage.builder().content(serializeResult(result).getBytes(UTF_8)).build());
    }

    private String serializeResult(Object result) throws JsonProcessingException {
        return JSON.stringify(result);
    }

    public static class SqlExecutorProvider implements MessageListenerProvider {

        @Override
        public MessageListener get() {
            return new SqlExecutor();
        }
    }

}
