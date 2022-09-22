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

package io.dingodb.calcite;

import io.dingodb.test.asserts.Assert;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TestSqlParser {
    @Test
    public void testSimple() throws SqlParseException {
        SqlParser parser = SqlParser.create("select 1");
        SqlNode sqlNode = parser.parseQuery();
        log.info("sqlNode = {}", sqlNode);
        SqlSelect select = (SqlSelect) Assert.sqlNode(sqlNode).kind(SqlKind.SELECT).getInstance();
        assertThat(select.getSelectList()).size().isEqualTo(1);
        Assert.sqlNode(select.getFrom()).isNull();
    }

    @Test
    public void testFullScan() throws SqlParseException {
        SqlParser parser = SqlParser.create("select * from test");
        SqlNode sqlNode = parser.parseQuery();
        log.info("sqlNode = {}", sqlNode);
        SqlSelect select = (SqlSelect) Assert.sqlNode(sqlNode).kind(SqlKind.SELECT).getInstance();
        assertThat(select.getSelectList()).size().isEqualTo(1);
        Assert.sqlNode(select.getFrom()).isTableName("TEST");
    }

    @Test
    public void testInsertValues() throws SqlParseException {
        SqlParser parser = SqlParser.create("insert into test values(1, 'Alice', 1.0)");
        SqlNode sqlNode = parser.parseQuery();
        log.info("sqlNode = {}", sqlNode);
        SqlInsert insert = (SqlInsert) Assert.sqlNode(sqlNode).kind(SqlKind.INSERT).getInstance();
        Assert.sqlNode(insert.getTargetTable()).isTableName("TEST");
    }
}
