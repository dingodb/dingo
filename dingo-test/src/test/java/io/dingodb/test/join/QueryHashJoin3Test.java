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

package io.dingodb.test.join;

import io.dingodb.test.SqlHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class QueryHashJoin3Test {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile(QueryHashJoin3Test.class.getResourceAsStream("table-students-create.sql"));
        sqlHelper.execFile(QueryHashJoin3Test.class.getResourceAsStream("table-classes-create.sql"));
        sqlHelper.execFile(QueryHashJoin3Test.class.getResourceAsStream("table-students-data.sql"));
        sqlHelper.execFile(QueryHashJoin3Test.class.getResourceAsStream("table-classes-data.sql"));
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

    @Test
    public void testStudentsClassesFullFilter() throws SQLException, IOException {
        sqlHelper.doTest(this.getClass(), "students-classes-full-join-filter");
    }

    @Test
    public void testStudentsClassesLeftFilter() throws SQLException, IOException {
        sqlHelper.doTest(this.getClass(), "students-classes-left-join-filter", "students-classes-full-join-filter");
    }

    @Test
    public void testStudentsClassesRightFilter() throws SQLException, IOException {
        sqlHelper.doTest(this.getClass(), "students-classes-right-join-filter", "students-classes-full-join-filter");
    }
}
