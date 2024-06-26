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

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

@Slf4j
public final class SessionManager {
    private SessionManager() {
    }

    public static java.sql.Connection getInnerConnection() {
        try {
            Class.forName("io.dingodb.driver.DingoDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        String user = "root";
        String host = "%";
        Properties properties = new Properties();
        properties.setProperty("defaultSchema", "DINGO");
        TimeZone timeZone = TimeZone.getDefault();
        properties.setProperty("timeZone", timeZone.getID());
        properties.setProperty("user", user);
        properties.setProperty("host", host);
        java.sql.Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:dingo:", properties);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return connection;
    }

    public static void update(String sql) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = SessionManager.getInnerConnection();
            statement = connection.createStatement();
            statement.executeUpdate(sql);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            SessionManager.close(null, statement, connection);
        }
    }

    public static void update(List<String> sqlList) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = SessionManager.getInnerConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            for (String sql : sqlList) {
                statement.executeUpdate(sql);
            }
            connection.setAutoCommit(true);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            SessionManager.close(null, statement, connection);
        }
    }

    public static void close(ResultSet rs, Statement statement, Connection connection) {
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
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
