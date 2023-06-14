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

package io.dingodb.calcite.grammar.dql;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlNextAutoIncrement extends SqlSelect {

    public SqlParserPos pos;

    public String schemaName;

    public String tableName;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("SELECT NEXT_AUTO_INCREMENT", SqlKind.SELECT);

    private static SqlNodeList of(SqlParserPos pos) {
        SqlIdentifier nextAutoIncrement = new SqlIdentifier("NEXT_AUTO_INCREMENT", pos);
        SqlNodeList sqlNodeList = new SqlNodeList(pos);
        sqlNodeList.add(nextAutoIncrement);
        return sqlNodeList;
    }

    /**
     * Creates a sqlDql.
     *
     * @param pos pos
     */
    public SqlNextAutoIncrement(SqlParserPos pos, SqlIdentifier tableName) {
        super(pos, null, of(pos), tableName,null,null,null,null,null,null, null,null);

        this.pos = pos;
        if (tableName.names.size() == 1) {
            this.tableName = tableName.names.get(0);
        } else {
            this.schemaName = tableName.names.get(0);
            this.tableName = tableName.names.get(1);
        }
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SELECT NEXT_AUTO_INCREMENT FROM");
        writer.keyword(tableName);
    }
}
