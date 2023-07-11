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

package io.dingodb.test;

import io.dingodb.common.type.DingoTypeFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.UUID;

@Slf4j
public class QueryTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    // TODO: currently the records overlap to each other for the key is empty if there is no primary key.
    @Test
    @Disabled
    public void testScan2() throws SQLException, IOException {
        sqlHelper.execFile("/table-test2-create.sql");
        sqlHelper.execFile("/table-test2-data.sql");
        String sql = "select * from test2";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "name", "amount"},
            DingoTypeFactory.tuple("INTEGER", "STRING", "DOUBLE"),
            "5, Emily, 5.5"
        );
        sqlHelper.clearTable("test2");
    }

    @Test
    public void testCancel() throws SQLException, IOException {
        sqlHelper.execFile("/table-test-create.sql");
        try (PreparedStatement preparedStatement = sqlHelper.getConnection().prepareStatement("insert into test values(?, ?, ?)")) {
            Random random = new Random();
            for (int i = 0; i < 10; ++i) {
                preparedStatement.setInt(1, i);
                preparedStatement.setString(2, UUID.randomUUID().toString());
                preparedStatement.setDouble(3, random.nextDouble());
                preparedStatement.execute();
            }
        }
        sqlHelper.execFile("/table-test-data.sql");
        try (Statement statement = sqlHelper.getConnection().createStatement()) {
            try (ResultSet resultSet = statement.executeQuery("select * from test")) {
                resultSet.next();
                statement.cancel();
                try {
                    Thread.sleep(1000);
                    resultSet.next(); // exception thrown.
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
