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
import org.apache.commons.lang3.StringUtils;

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
        SqlIntervalQualifier sqlIntervalQualifier = interval.getIntervalQualifier();
        String intervalAlias = sqlIntervalQualifier.getAliasString("intervalAlias");
        if (StringUtils.isEmpty(intervalAlias)) {
            writer.keyword("INTERVAL");
        } else {
            writer.keyword(intervalAlias);
        }
        if (interval.getSign() == -1) {
            writer.print("-");
        }
        String intervalLiteralAlias = sqlIntervalQualifier.getAliasString("intervalLiteralAlias");
        if (StringUtils.isEmpty(intervalLiteralAlias)) {
            writer.literal("'" + interval.getIntervalLiteral() + "'");
        } else {
            writer.literal(intervalLiteralAlias);
        }
        String timeUnitRange = sqlIntervalQualifier.getAliasString("aliasName");
        if (StringUtils.isEmpty(timeUnitRange)) {
            unparseSqlIntervalQualifier(writer, interval.getIntervalQualifier(),
                DingoRelDataTypeSystemImpl.DEFAULT);
        } else {
            writer.keyword(timeUnitRange);
        }
    }
}
