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

package org.apache.calcite.sql;

import io.dingodb.calcite.type.DingoRelDataTypeSystemImpl;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;

public class DingoAnsiSqlDialect extends AnsiSqlDialect {

    public static final SqlDialect DEFAULT = new DingoAnsiSqlDialect(DEFAULT_CONTEXT);

    /**
     * Creates an AnsiSqlDialect.
     *
     * @param context
     */
    public DingoAnsiSqlDialect(Context context) {
        super(context);
    }

    @Override
    public void unparseSqlIntervalLiteral(SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
        SqlIntervalLiteral.IntervalValue interval =
            literal.getValueAs(SqlIntervalLiteral.IntervalValue.class);
        writer.keyword("INTERVAL");
        if (interval.getSign() == -1) {
            writer.print("-");
        }
        writer.literal("'" + interval.getIntervalLiteral() + "'");
        unparseSqlIntervalQualifier(writer, interval.getIntervalQualifier(),
            DingoRelDataTypeSystemImpl.DEFAULT);
    }
}
