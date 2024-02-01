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

import io.dingodb.test.dsl.run.exec.SqlExecContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.SQLException;

import static io.dingodb.test.dsl.builder.SqlTestCaseJavaBuilder.csv;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AutoIncrementTest {
    private SqlExecContext context;

    @BeforeAll
    public static void setupAll() throws Exception {
        ConnectionFactory.initLocalEnvironment();
    }

    @AfterAll
    public static void cleanUpAll() {
        ConnectionFactory.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
        context = new SqlExecContext(ConnectionFactory.getConnection());
    }

    @AfterEach
    public void cleanUp() throws Exception {
        context.cleanUp();
    }

    @Disabled("This should be fixed.")
    @Test
    public void test() throws SQLException {
        context.execSql(
            "create table {table}("
                + "id int auto_increment, "
                + "name varchar(32), "
                + "age int,"
                + "primary key (id)"
                + ") partition by range values (2),(3)"
        );
        context.execSql("insert into {table}(name, age) values('a', 23), ('Billy', 19)");
        context.execSql("select * from {table} order by id")
            .test(csv(
                "ID, NAME, AGE",
                "INT, STRING, INT",
                "1, a, 23",
                "2, Billy, 19"
            ));
    }
}
