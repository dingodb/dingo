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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class QueryLikeWithPrefixTest {

    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-like-create2.sql");
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
        sqlHelper.execFile("/table-test-like-data2.sql");
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    //@Disabled
    @Test
    public void testLike() throws SQLException, IOException {
        // abc%c%
        sqlHelper.queryTest("select * from t_like2 where name like 'abc%c%'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "1, abcdec\n"
                + "2, abcdecf\n"
                + "3, abcdcf\n"
                + "4, abcdcef\n"
        );
    }

    //@Disabled
    @Test
    public void testLike2() throws SQLException, IOException {
        // abc_c_
        sqlHelper.queryTest("select * from t_like2 where name like 'abc_C_'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "3, abcdcf\n"
        );
    }

    //@Disabled
    @Test
    public void testLike3() throws SQLException, IOException {
        // a[a-zA-Z]c
        sqlHelper.queryTest("select * from t_like2 where name like 'a[a-zA-Z]C'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "5, asc\n"
        );
    }

    //@Disabled
    @Test
    public void testLike4() throws SQLException, IOException {
        // a[^a-z]c
        sqlHelper.queryTest("select * from t_like2 where name like 'a[^a-z]c'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "6, a6c\n"
        );
    }

    //@Disabled
    @Test
    public void testLike5() throws SQLException, IOException {
        // a[^a-z]c
        sqlHelper.queryTest("select * from t_like2 where id like '1%'",
            new String[]{"id", "name"},
            DingoTypeFactory.tuple("INTEGER", "STRING"),
            "1, abcdec\n"
        );
    }
}
