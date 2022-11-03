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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class QueryLikeTest {

    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-like-create.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execFile("/table-test-like-data.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    @Test
    public void testLike() throws SQLException, IOException {
        // %abc%c%
        sqlHelper.queryTest("select * from t_like where name like '%abc%C%'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "1, 1abcdec\n"
                + "2, 2abcdecf\n"
                + "3, 3abcdcf\n"
                + "4, 4abcdcef\n"
        );
    }

    @Test
    public void testLike2() throws SQLException, IOException {
        // _abc_c_
        sqlHelper.queryTest("select * from t_like where name like '_abc_C_'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "3, 3abcdcf\n"
        );
    }

    @Test
    public void testLike3() throws SQLException, IOException {
        // [a-zA-Z]c
        sqlHelper.queryTest("select * from t_like where name like '[a-zA-Z]C'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "5, sc\n"
        );
    }

    @Test
    public void testLike4() throws SQLException, IOException {
        // [^a-z]a
        sqlHelper.queryTest("select * from t_like where name like '[^a-z]c'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "6, 6c\n"
        );
    }

    @Test
    public void testLike5() throws SQLException, IOException {
        // Data contains %
        sqlHelper.queryTest("select * from t_like where name like '%a\\%b_'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "7, 7a%bc\n"
        );
    }

    @Test
    public void testLike6() throws SQLException, IOException {
        // Data contains _
        sqlHelper.queryTest("select * from t_like where name like '_a\\_b%'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "8, 8a_bcd\n"
        );
    }

    @Test
    public void testLikeBinary() throws SQLException, IOException {
        // %abc%C%
        sqlHelper.queryTest("select * from t_like where name like binary '%abc%C%'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            ""
        );
    }
}
