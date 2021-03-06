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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

@Slf4j
public class BasicTypeWithTimeStampTest {
    private static final String TEST_ALL_DATA
        = "1, Alice, 2020-01-01 00:00:01\n"
        + "2, Betty, 2020-01-02 00:01:02\n";
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-create-with-timestamp.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execFile("/table-test-data-with-timestamp.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    @Test
    public void testScan() throws SQLException, IOException {
        sqlHelper.queryTest("select * from test",
            new String[]{"id", "name", "birth"},
            DingoTypeFactory.tuple("INTEGER", "STRING", "TIMESTAMP"),
            TEST_ALL_DATA
        );
    }
}
