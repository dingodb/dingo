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

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlCreateIndex extends SqlCreate {

    public String index;

    public SqlIdentifier table;

    public List<SqlIdentifier> columns;

    public boolean isUnique;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE INDEX", SqlKind.OTHER_DDL);

    public SqlCreateIndex(SqlParserPos pos, boolean replace, boolean ifNotExists,
                          String index,
                          SqlIdentifier table,
                          List<SqlIdentifier> columns,
                          boolean isUnique) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.index = index;
        this.table = table;
        this.columns = columns;
        this.isUnique = isUnique;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("create index");
        writer.keyword(index);
    }

    public String[] getColumnNames() {
        String[] columnNames = columns.stream()
            .map(SqlIdentifier::getSimple)
            .map(String::toUpperCase)
            .toArray(String[]::new);
        return columnNames;
    }
}
