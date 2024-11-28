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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterColumn extends SqlAlterTable {

    int op = 0;
    SqlNode defaultExpr;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER TABLE ALTER COLUMN", SqlKind.ALTER_TABLE);

    public SqlAlterColumn(SqlParserPos pos, SqlIdentifier sqlIdentifier, int op, SqlNode defaultExpr) {
        super(pos, sqlIdentifier, OPERATOR);
        this.op = op;
        this.defaultExpr = defaultExpr;
    }
}
