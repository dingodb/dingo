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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class DingoSqlSetOptions extends SqlAlter {
    protected static final SqlSpecialOperator OPERATOR;

    public List<SqlSetOption> sqlSetOptionList;

    public DingoSqlSetOptions(SqlParserPos pos, @Nullable String scope, List<SqlSetOption> sqlSetOptionList) {
        super(pos, scope);
        this.sqlSetOptionList = sqlSetOptionList;
    }

    @Override
    protected void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.sqlSetOptionList != null) {
            writer.keyword("SET");
        } else {
            writer.keyword("RESET");
        }
        for (int i = 0; i < sqlSetOptionList.size(); i ++) {
            SqlSetOption sqlSetOption = sqlSetOptionList.get(i);
            if ("SYSTEM".equals(sqlSetOption.getScope())) {
                writer.keyword("GLOBAL");
            } else if (sqlSetOption.getScope() != null) {
                writer.keyword(sqlSetOption.getScope());
            }
            sqlSetOption.getName().unparse(writer, leftPrec, rightPrec);
            if (sqlSetOption.getValue() != null) {
                sqlSetOption.getValue().unparse(writer, leftPrec, rightPrec);
            }
            if (i < sqlSetOptionList.size() - 1) {
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
        if (sqlSetOptionList.isEmpty()) {
            return List.of();
        }
        List<SqlNode> operandList = new ArrayList<>();
        SqlSetOption sqlSetOption = sqlSetOptionList.get(0);
        if (sqlSetOption.getScope() == null) {
            operandList.add((SqlNode) null);
        } else {
            operandList.add(new SqlIdentifier(sqlSetOption.getScope(), SqlParserPos.ZERO));
        }

        operandList.add(sqlSetOption.getName());
        operandList.add(sqlSetOption.getValue());
        return ImmutableNullableList.copyOf(operandList);
    }

    static {
        OPERATOR = new SqlSpecialOperator("SET_OPTIONS", SqlKind.SET_OPTION) {
            public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
                SqlNode scopeNode = operands[0];
                return new SqlSetOption(pos, scopeNode == null ? null : scopeNode.toString(), (SqlIdentifier)operands[1], operands[2]);
            }
        };
    }
}
