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
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlFlashBackTable extends SqlFlashback {

    public String schemaName;

    public String tableName;

    public String newSchemaName;
    public String newTableName;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("FLASHBACK TABLE", SqlKind.OTHER_DDL);

    public SqlFlashBackTable(SqlParserPos pos, SqlIdentifier tableId, SqlIdentifier newTableId) {
        super(OPERATOR, pos);
        if (tableId.names.size() > 1) {
            this.schemaName = tableId.names.get(0);
            this.tableName = tableId.names.get(1);
        } else {
            this.tableName = tableId.names.get(0);
        }
        if (newTableId.names.size() > 1) {
            this.newSchemaName = newTableId.names.get(0);
            this.newTableName = newTableId.names.get(1);
        } else {
            this.newTableName = newTableId.names.get(0);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("FLASHBACK TABLE ");
        writer.keyword(tableName);
    }
}
