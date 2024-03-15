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
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlKill extends SqlDdl {

    @Getter
    protected String threadId;

    /**
     * Creates a SqlDdl.
     *
     * @param operator
     * @param pos
     */
    public SqlKill(SqlOperator operator, SqlParserPos pos, String threadId) {
        super(operator, pos);
        if (threadId.startsWith("'") && threadId.endsWith("'")) {
            threadId = threadId.replace("'", "");
        }
        this.threadId = threadId;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }
}
