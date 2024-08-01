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

package io.dingodb.common.session;

import io.dingodb.common.log.LogUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Data
@AllArgsConstructor
@Slf4j
public class Session {
    private SessionContext sessionContext;
    private Connection connection;

    public void setAutoCommit(boolean autoCommit) {
        try {
            connection.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            LogUtils.error(log, e.getMessage());
        }
    }

    public String executeUpdate(String sql) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(sql);
            return null;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage() + ",sql:{}" , e, sql);
            return e.getMessage();
        } finally {
            close(null, statement);
        }
    }

    public List<Object[]> executeQuery(String sql) throws SQLException {
        ResultSet rs = null;
        Statement statement = null;
        List<Object[]> objectList = new ArrayList<>();
        try {
            statement = connection.createStatement();
            rs = statement.executeQuery(sql);
            ResultSetMetaData rm = rs.getMetaData();
            int count = rm.getColumnCount();
            while (rs.next()) {
                Object[] row = new Object[count];
                for (int i = 1; i <= count; i ++) {
                    row[i - 1] = rs.getObject(i);
                }
                objectList.add(row);
            }
        } finally {
            close(rs, statement);
        }
        return objectList;
    }

    public void executeUpdate(List<String> sqlList) {
        Statement statement = null;
        try {
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            for (String sql : sqlList) {
                statement.executeUpdate(sql);
            }
            connection.setAutoCommit(true);
        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            log.error(e.getMessage(), e);
        } finally {
            close(null, statement);
        }
    }

    public void runInTxn(Function<Session, Exception> f) {
        try {
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Exception exception = f.apply(this);
        if (exception != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            this.connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        try {
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void close(ResultSet rs, Statement statement) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void destroy() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean validate() {
        try {
            return !connection.isClosed();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
