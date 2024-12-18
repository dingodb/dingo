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

import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

public class SqlAlterSchema extends SqlAlter {
    public SqlIdentifier schema;
    public String charset;
    public String collate;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER DATABASE", SqlKind.ALTER_TABLE);

    public SqlAlterSchema(SqlParserPos pos, SqlIdentifier schema, String charset, String collate) {
        super(pos);
        this.schema = schema;
        this.charset = charset;
        this.collate = collate;
    }

    @Override
    protected void unparseAlterOperation(SqlWriter sqlWriter, int left, int right) {
        sqlWriter.keyword("ALTER DATABASE");
        schema.unparse(sqlWriter, left, right);
        if (charset != null) {
            sqlWriter.keyword("DEFAULT CHARACTER SET=");
            sqlWriter.keyword(charset);
        }
        if (collate != null) {
            sqlWriter.keyword("COLLATE=");
            sqlWriter.keyword(collate);
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.emptyList();
    }
}
