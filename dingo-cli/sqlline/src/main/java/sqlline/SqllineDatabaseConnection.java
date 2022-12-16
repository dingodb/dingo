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

package sqlline;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

public class SqllineDatabaseConnection extends DatabaseConnection {

    public static final String DRIVER = "dingo";
    public static final String URL = "jdbc:dingo::///";
    public static final Properties PROPERTIES = new Properties();

    private final SqlLine sqlLine;

    public SqllineDatabaseConnection(SqlLine sqlLine, Connection connection) throws SQLException {
        super(sqlLine, DRIVER, URL, null, null, PROPERTIES);
        this.sqlLine = sqlLine;
        this.connection = connection;
        meta = (DatabaseMetaData) Proxy.newProxyInstance(
            DatabaseMetaData.class.getClassLoader(),
            new Class[]{DatabaseMetaData.class},
            new DatabaseMetaDataHandler(connection.getMetaData()));
    }

    @Override
    boolean connect() throws SQLException {
        return true;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }
}
