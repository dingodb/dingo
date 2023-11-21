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

import lombok.Getter;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlLockBlock extends SqlLock {

    @Getter
    private List<SqlBlock> sqlBlockList;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("LOCK BLOCK", SqlKind.OTHER_DDL);

    /**
     * Creates a SqlDdl.
     *
     * @param pos
     */
    public SqlLockBlock(SqlParserPos pos, List<SqlBlock> sqlBlockList) {
        super(OPERATOR, pos);
        this.sqlBlockList = sqlBlockList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("LOCK BLOCKS");
        for (int j = 0; j < sqlBlockList.size(); j ++) {
            SqlBlock block = sqlBlockList.get(j);
            writer.keyword(block.table);
            if (block.getFuncName().equalsIgnoreCase("HASH")) {
                writer.keyword(block.getFuncName());
                writer.keyword("(");
                writer.keyword(block.getHash().toString());
                writer.keyword(")");
            }
            writer.keyword("RANGE");
            writer.keyword("(");
            for (int i = 0; i < block.operands.length; i ++) {
                writer.keyword(block.operands[i].toString());
                if (i < block.operands.length - 1) {
                    writer.keyword(",");
                }
            }
            writer.keyword(")");
            writer.keyword("AS");
            writer.keyword(block.name);

            if (j < sqlBlockList.size() - 1) {
               writer.keyword(",");
            }
        }
    }
}
