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

public class ShowTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-test-auto-increment-create.sql");
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
    @Disabled
    public void showCreateTable() throws SQLException, IOException {
        String sql = "show create table t_auto";

        sqlHelper.queryTest(
            sql,
            new String[]{"Table", "Create Table"},
            DingoTypeFactory.tuple("VARCHAR", "VARCHAR"),
            "T_AUTO, CREATE TABLE `t_auto` (`id` INTEGER',' `name` VARCHAR(32)',' `age` INTEGER',' PRIMARY KEY (`id`))");
    }

    @Test
    @Disabled
    public void showAllColumns() throws SQLException, IOException {
        String sql = "show columns from t_auto";

        sqlHelper.queryTest(
            sql,
            new String[]{"Field", "Type", "Null", "Key", "Default"},
            DingoTypeFactory.tuple("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"),
            "id, INT, NO, PRI, \n"
            + "name, STRING|NULL, YES, , \n"
            + "age, INT|NULL, YES, , ");
    }

    @Test
    @Disabled
    public void showAllColumnsWithLike() throws SQLException, IOException {
        String sql = "show columns from t_auto like 'na%'";

        sqlHelper.queryTest(
            sql,
            new String[]{"Field", "Type", "Null", "Key", "Default"},
            DingoTypeFactory.tuple("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"),
            "name, STRING|NULL, YES, ,");
    }

    @Test
    @Disabled
    public void showTableDistribution() throws SQLException, IOException {
        String sql = "show table t_auto distribution";

        sqlHelper.queryTest(
            sql,
            new String[]{"Id", "Type", "Value"},
            DingoTypeFactory.tuple("VARCHAR", "VARCHAR", "VARCHAR"),
            "id, Range, [ Key(1), Key(3) )");
    }
}
