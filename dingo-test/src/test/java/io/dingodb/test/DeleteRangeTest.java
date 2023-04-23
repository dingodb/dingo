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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class DeleteRangeTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-between-create.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execFile("/table-test-between-data.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {

    }

    @Test
    public void testDelete1() throws SQLException, IOException {
        sqlHelper.updateTest("delete from t_ba where id > 2", 13);
    }

    @Test
    public void testDelete2() throws SQLException, IOException {
        sqlHelper.updateTest("delete from t_ba where id > 2 and id < 10", 7);
    }

    @Test
    public void testDelete3() throws SQLException, IOException {
        sqlHelper.updateTest("delete from t_ba where id < 10", 9);
    }
}
