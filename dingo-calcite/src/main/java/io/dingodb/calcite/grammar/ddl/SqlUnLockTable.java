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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlUnLockTable extends SqlUnLock {

    public List<String> tableList;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("UNLOCK TABLE", SqlKind.OTHER_DDL);

    /**
     * Creates a SqlDdl.
     *
     * @param pos
     */
    public SqlUnLockTable(SqlParserPos pos, List<String> tableList) {
        super(OPERATOR, pos);
        this.tableList = tableList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("UNLOCK TABLES");
        for (int i = 0; i < tableList.size(); i ++) {
            writer.keyword(tableList.get(i));
            if (i < tableList.size() - 1) {
                writer.keyword(",");
            }
        }
    }
}
