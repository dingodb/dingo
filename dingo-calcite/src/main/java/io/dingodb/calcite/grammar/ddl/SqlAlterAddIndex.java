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
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlAlterAddIndex extends SqlAlterTable {

    public String type;

    public String index;

    public List<SqlIdentifier> columns;

    public boolean isUnique;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE INDEX", SqlKind.OTHER_DDL);

    public SqlAlterAddIndex(SqlParserPos pos, SqlIdentifier sqlIdentifier, String index,
                            List<SqlIdentifier> columns, Boolean isUnique) {
        super(pos, sqlIdentifier, OPERATOR);
        this.index = index;
        this.columns = columns;
        this.isUnique = isUnique;
    }

    public String[] getColumnNames() {
        String[] columnNames = columns.stream().map(column -> column.getSimple())
            .map(String::toUpperCase)
            .toArray(String[]::new);
        return columnNames;
    }
}
