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

package io.dingodb.calcite.grammar.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.ddl.DingoSqlColumn;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

public class SqlAlterModifyColumn extends SqlAlterTable {

    public List<DingoSqlColumn> dingoSqlColumnList = new ArrayList<>();

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER TABLE MODIFY COLUMN", SqlKind.ALTER_TABLE);

    public SqlAlterModifyColumn(SqlParserPos pos, SqlIdentifier sqlIdentifier, DingoSqlColumn sqlColumn) {
        super(pos, sqlIdentifier, OPERATOR);
        this.dingoSqlColumnList.add(sqlColumn);
    }

    public void addSqlColumn(DingoSqlColumn sqlColumn) {
        this.dingoSqlColumnList.add(sqlColumn);
    }
}
