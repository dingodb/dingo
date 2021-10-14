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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryPlanTest {
    private static Connection connection;

    @BeforeAll
    public static void setupAll() throws ClassNotFoundException, SQLException {
        Class.forName(DingoDriver.class.getCanonicalName());
        connection = DriverManager.getConnection("jdbc:dingo:");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        connection.close();
    }

    @Disabled
    @Test
    public void testSimpleValues() throws SQLException {
        String sql = "select 1";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                }
            }
        }
    }
}
