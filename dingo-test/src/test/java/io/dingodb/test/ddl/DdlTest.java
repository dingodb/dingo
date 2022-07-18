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

package io.dingodb.test.ddl;

import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.sql.SQLException;
import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class DdlTest {
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

    @ParameterizedTest
    @CsvSource({
        "int, 100",
        "bigint, 10000",
        "char, abc",
        "varchar, def",
        "boolean, true",
        "float, 3.5",
        "double, 2.7",
        "date, 1970-1-1",
        "time, 00:00:00",
        "timestamp, 1970-1-1 00:00:00",
    })
    public void testCreateTable(@Nonnull String type, String value) throws SQLException {
        String tableName = SqlHelper.randomTableName();
        String sql = "create table " + tableName + "(\n"
            + "    id int,\n"
            + "    data " + type + ",\n"
            + "    primary key(id)\n"
            + ")";
        sqlHelper.execSqlCmd(sql);
        int typeCode = TypeCode.codeOf(type.toUpperCase());
        String valueLiteral = value;
        if (
            typeCode == TypeCode.STRING
                || typeCode == TypeCode.DATE
                || typeCode == TypeCode.TIME
                || typeCode == TypeCode.TIMESTAMP
        ) {
            valueLiteral = "'" + value + "'";
        }
        sql = "insert into " + tableName + " values(1, " + valueLiteral + ")";
        sqlHelper.execSqlCmd(sql);
        Object result = sqlHelper.querySimpleValue("select data from " + tableName);
        Object expected = DingoTypeFactory.scalar(typeCode, false).parse(value);
        if (typeCode == TypeCode.DATE) {
            assertThat(result.toString()).isEqualTo(expected.toString());
        } else {
            assertThat(result).isEqualTo(expected);
        }
        sqlHelper.dropTable(tableName);
    }

    @Test
    public void test() throws SQLException, IOException {
        sqlHelper.execFile(DdlTest.class.getResourceAsStream("table-non-scalar-create.sql"));
    }
}
