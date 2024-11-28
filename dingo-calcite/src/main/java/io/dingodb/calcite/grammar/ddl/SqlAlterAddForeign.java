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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAlterAddForeign extends SqlAlterTable {

    SqlIdentifier table;
    SqlIdentifier foreignName;
    SqlNodeList columnList;
    SqlNodeList refColumnList;
    SqlIdentifier refTable;

    String updateRefOpt;
    String deleteRefOpt;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER TABLE ADD CONSTRAINT FOREIGN", SqlKind.ALTER_TABLE);

    public SqlAlterAddForeign(
        SqlParserPos pos,
        SqlIdentifier sqlIdentifier,
        SqlIdentifier foreignName,
        SqlNodeList columnList,
        SqlIdentifier refTable,
        SqlNodeList refColumnList,
        String updateRefOpt,
        String deleteRefOpt
    ) {
        super(pos, sqlIdentifier, OPERATOR);
        this.table = sqlIdentifier;
        this.foreignName = foreignName;
        this.columnList = columnList;
        this.refTable = refTable;
        this.refColumnList = refColumnList;
        this.updateRefOpt = updateRefOpt;
        this.deleteRefOpt = deleteRefOpt;
    }
}
