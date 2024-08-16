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
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

@Slf4j
public class SessionFactory extends BasePooledObjectFactory<Session> {

    @Override
    public Session create() throws Exception {
        return new Session(new SessionContext(), getInternalConnection());
    }

    @Override
    public PooledObject<Session> wrap(Session o) {
        return new DefaultPooledObject<>(o);
    }

    public java.sql.Connection getInternalConnection() {
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
        properties.setProperty("sql_log", "off");
        java.sql.Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:dingo:", properties);
            connection.setClientInfo("ddl_inner_profile", "on");
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        return connection;
    }
}
