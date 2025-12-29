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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlAlterAddPrimaryKey extends SqlAlterTable {

    SqlNodeList sqlIdentifierList;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER TABLE ADD PRIMARY KEY", SqlKind.ALTER_TABLE);

    public SqlAlterAddPrimaryKey(SqlParserPos pos, SqlIdentifier sqlIdentifier, SqlNodeList sqlIdentifierList) {
        super(pos, sqlIdentifier, OPERATOR);
        this.sqlIdentifierList = sqlIdentifierList;
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ADD PRIMARY KEY");
        writer.keyword(sqlIdentifierList.toString());
    }

    public List<String> getKeyList() {
        return sqlIdentifierList.stream().map(SqlNode::toString).toList();
    }
}
