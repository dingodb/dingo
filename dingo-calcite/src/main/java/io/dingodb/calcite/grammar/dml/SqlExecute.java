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

package io.dingodb.calcite.grammar.dml;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.stream.Collectors;

public class SqlExecute extends SqlCall {
    public String statementName;
    public List<String> parameters;
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("EXECUTE", SqlKind.OTHER);

    public SqlExecute(SqlParserPos pos, String statementName, List<String> parameters) {
        super(pos);
        this.statementName = statementName;
        this.parameters = parameters.stream().map(param -> "@" + param).collect(Collectors.toList());
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("EXECUTE");
        writer.keyword(statementName);
        writer.keyword("USING");
        for (int i = 0; i < parameters.size(); i++) {
            if (i != 0) {
                writer.keyword(",");
            }
            writer.print(parameters.get(i));
        }
    }
}
