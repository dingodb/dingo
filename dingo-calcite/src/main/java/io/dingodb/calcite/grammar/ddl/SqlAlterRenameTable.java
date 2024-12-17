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

public class SqlAlterRenameTable extends SqlAlter {

    public List<SqlIdentifier> originIdList;
    public List<SqlIdentifier> toIdList;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("RENAME TABLE", SqlKind.ALTER_TABLE);

    public SqlAlterRenameTable(SqlParserPos pos, List<SqlIdentifier> sqlIdentifier, List<SqlIdentifier> toIdList) {
        super(pos);
        this.originIdList = sqlIdentifier;
        this.toIdList = toIdList;
    }

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("RENAME TABLE");
        for (int i = 0; i < originIdList.size(); i ++) {
            originIdList.get(i).unparse(writer, leftPrec, rightPrec);
            writer.keyword("TO");
            toIdList.get(i).unparse(writer, leftPrec, rightPrec);
            if (i < originIdList.size() - 1) {
                writer.keyword(",");
            }
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
