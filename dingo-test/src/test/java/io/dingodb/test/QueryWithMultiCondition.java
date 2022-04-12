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

import io.dingodb.common.table.TupleSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class QueryWithMultiCondition {

    @Test
    public void testCaseWithOutPrimaryKeys() throws IOException, SQLException {
        SqlHelper sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-create.sql");
        sqlHelper.execFile("/table-test-data.sql");
        String sql = "select id, name from test where name in ('Alice', 'AAAA') and amount > 6" ;
        sqlHelper.queryTest(
                sql,
                new String[]{"id", "name"},
                TupleSchema.ofTypes("INTEGER", "STRING"),
                "8, Alice"
        );
        sqlHelper.cleanUp();
    }

    @Test
    public void testCaseHasPartPrimaryKey() throws IOException, SQLException {
        SqlHelper sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-create.sql");
        sqlHelper.execFile("/table-test-data.sql");
        String sql = "select id, name from test where id in (1,2) and amount > 3.6" ;
        sqlHelper.queryTest(
                sql,
                new String[]{"id", "name"},
                TupleSchema.ofTypes("INTEGER", "STRING"),
                "2, Betty"
        );
        sqlHelper.cleanUp();
    }

    @Test
    public void testCaseHasAllPrimaryKey() throws IOException, SQLException {
        SqlHelper sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-with-multi-key-create.sql");
        sqlHelper.execFile("/table-test-data.sql");
        String sql = "select id, name from test where id in (1,2,3,4) and name in ('Alice', 'Betty1')" ;
        sqlHelper.queryTest(
                sql,
                new String[]{"id", "name"},
                TupleSchema.ofTypes("INTEGER", "STRING"),
                "1, Alice"
        );
        sqlHelper.cleanUp();
    }

    @Test
    public void testCaseHasAllKeyAndOthers() throws IOException, SQLException {
        SqlHelper sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-with-multi-key-create.sql");
        sqlHelper.execFile("/table-test-data.sql");
        String sql = "select id, name from test where id in (1,2,3,4) and name in ('Alice', 'Betty1') and amount > 0" ;
        sqlHelper.queryTest(
                sql,
                new String[]{"id", "name"},
                TupleSchema.ofTypes("INTEGER", "STRING"),
                "1, Alice"
        );
        sqlHelper.cleanUp();
    }
}
