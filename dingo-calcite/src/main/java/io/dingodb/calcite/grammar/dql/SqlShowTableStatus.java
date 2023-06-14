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

package io.dingodb.calcite.grammar.dql;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

public class SqlShowTableStatus extends SqlShow {
    public String schema;
    public String sqlLikePattern;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SHOW TABLE STATUS", SqlKind.SELECT);

    /**
     * Creates a SqlDdl.
     *
     * @param pos      pos
     */
    public SqlShowTableStatus(SqlParserPos pos, String schema, String sqlLikePattern) {
        super(OPERATOR, pos);
        this.schema = schema;
        this.sqlLikePattern = sqlLikePattern;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW");
        writer.keyword("TABLE");
        writer.keyword("STATUS");
        if (StringUtils.isNotBlank(schema)) {
            writer.keyword("FROM");
            writer.keyword(schema);
        }
        if (StringUtils.isNotBlank(sqlLikePattern)) {
            writer.keyword("LIKE");
            writer.keyword(sqlLikePattern);
        }
    }
}
