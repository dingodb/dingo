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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlShowIndexFromTable extends SqlShow {
    public String schemaName;
    public String tableName;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("SHOW INDEX FROM TABLE", SqlKind.SELECT);

    /**
     * Creates a SqlDdl.
     *
     * @param pos      pos
     */
    public SqlShowIndexFromTable(SqlParserPos pos, SqlIdentifier tableName) {
        super(OPERATOR, pos);
        if (tableName.names.size() == 1) {
            this.tableName = tableName.names.get(0);
        } else {
            this.schemaName = tableName.names.get(0).toUpperCase();
            this.tableName = tableName.names.get(1);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW INDEX FROM");
        writer.keyword(schemaName + "." + tableName);
    }
}
