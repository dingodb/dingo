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

package io.dingodb.test.dsl.run;

import io.dingodb.test.dsl.run.exec.Exec;
import io.dingodb.test.dsl.run.exec.ExecSql;
import io.dingodb.test.dsl.run.exec.SqlExecContext;
import io.dingodb.test.utils.TableUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class SqlRunningContext {
    private final Connection connection;
    private final Map<String, TableInfo> tables;

    public SqlRunningContext(Connection connection) {
        this.connection = connection;
        this.tables = new ConcurrentHashMap<>();
    }

    public void put(String tableId, TableInfo tableInfo) {
        tables.put(tableId, tableInfo);
    }

    public void setModified(String tableId) {
        tables.get(tableId).setPopulated(false);
    }

    public void cleanUp() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (Map.Entry<String, TableInfo> entry : tables.entrySet()) {
                String tableName = entry.getValue().getTableName();
                if (tableName != null) {
                    statement.execute("drop table " + tableName);
                }
            }
        }
    }

    /**
     * Prepare table for running, synchronized for there may be tests running in parallel.
     *
     * @param tableId the table id
     * @return the random name of the table created
     * @throws SQLException sql exception
     */
    public synchronized String prepareTable(String tableId) throws SQLException {
        TableInfo tableInfo = tables.get(tableId);
        if (tableInfo == null) {
            throw new RuntimeException(
                "Table \"" + tableId + "\" not found, forget to init tables or use wrong table id?"
            );
        }
        boolean isNewCreated = false;
        if (tableInfo.getTableName() == null) {
            SqlExecContext execContext = new SqlExecContext(connection);
            tableInfo.getCreate().run(execContext);
            String tableName = execContext.getTableName(TableUtils.DEFAULT_TABLE_PLACEHOLDER_NAME);
            if (tableName == null) {
                throw new RuntimeException(
                    "Cannot get created table name, placeholder {"
                        + TableUtils.DEFAULT_TABLE_PLACEHOLDER_NAME
                        + "} must be used."
                );
            }
            tableInfo.setTableName(tableName);
            isNewCreated = true;
        }
        if (!tableInfo.isPopulated()) {
            SqlExecContext execContext = new SqlExecContext(connection);
            execContext.addTableMapping(TableUtils.DEFAULT_TABLE_PLACEHOLDER_NAME, tableInfo.getTableName());
            if (!isNewCreated) {
                new ExecSql("truncate {" + TableUtils.DEFAULT_TABLE_PLACEHOLDER_NAME + "}").run(execContext);
            }
            Exec exec = tableInfo.getInit();
            if (exec != null) {
                tableInfo.getInit().run(execContext);
            }
            tableInfo.setPopulated(true);
        }
        return tableInfo.getTableName();
    }

    public @NonNull SqlExecContext prepareExecContext(
        @NonNull Map<String, String> preparedTableMapping
    ) throws SQLException {
        Map<String, String> tableMapping = new HashMap<>(1);
        for (Map.Entry<String, String> entry : preparedTableMapping.entrySet()) {
            String tableName = prepareTable(entry.getValue());
            tableMapping.put(entry.getKey(), tableName);
        }
        return new SqlExecContext(connection, tableMapping);
    }
}
