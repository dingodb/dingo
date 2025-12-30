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

package org.apache.calcite.util;

import com.google.common.base.Preconditions;
import io.dingodb.calcite.type.DingoIntervalLiteral;
import io.dingodb.calcite.type.DingoRelDataTypeSystemImpl;
import io.dingodb.calcite.type.DingoSqlLiteral;
import io.dingodb.expr.common.utils.CastWithString;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.math.BigDecimal;

public final class DingoParserUtils {

    public static DingoIntervalLiteral parseIntervalLiteral(SqlParserPos pos,
                                                            int sign,
                                                            String s,
                                                            SqlIntervalQualifier intervalQualifier) {
        if (s != null) {
            if (s.contains(".")) {
                s = s.split("\\.")[0];
            }
            if (s.isEmpty()) {
                s = "0";
            }
            s = String.valueOf(CastWithString.longCastWithStringCompat(s));
            if (sign == -1) {
                s = "-" + s;
            }
        }
        return DingoSqlLiteral.createInterval(sign, s, intervalQualifier, pos);
    }

    public static DingoIntervalLiteral parseIntervalNumericLiteral(SqlParserPos pos,
                                                            int sign,
                                                            BigDecimal value,
                                                            SqlIntervalQualifier intervalQualifier) {
        if (sign > 0) {
            if (value.compareTo(new BigDecimal(Long.MAX_VALUE / 1000)) > 0) {
                throw new RuntimeException("Interval parameter is larger than the maximum range: 9223372036854775");
            }
        } else {
            if (new BigDecimal("-" + value.toString()).compareTo(new BigDecimal(Long.MIN_VALUE / 1000)) < 0) {
                throw new RuntimeException("Interval parameter is smaller than the minimum range: -9223372036854775");
            }
        }
        String s = String.valueOf(sign * Math.round(value.doubleValue()));
        return DingoSqlLiteral.createInterval(sign, s, intervalQualifier, pos);
    }

    public static long intervalToMonths(SqlIntervalLiteral.IntervalValue interval) {
        return intervalToMonths(
            interval.getIntervalLiteral(),
            interval.getIntervalQualifier());
    }

    public static long intervalToMonths(
        String literal,
        SqlIntervalQualifier intervalQualifier) {
        Preconditions.checkArgument(intervalQualifier.isYearMonth(),
            "interval must be year month");
        long[] ret;
        try {
            ret = intervalQualifier.evaluateIntervalLiteral(literal,
                intervalQualifier.getParserPosition(), DingoRelDataTypeSystemImpl.DEFAULT);
            assert ret != null;
        } catch (CalciteContextException e) {
            throw new RuntimeException("Error while parsing year-to-month interval " + literal, e);
        }

        long l = 0;
        long[] conv = new long[2];
        conv[1] = 1; // months
        conv[0] = conv[1] * 12; // years
        for (int i = 1; i < ret.length; i++) {
            l += conv[i - 1] * ret[i];
        }
        return ret[0] * l;
    }

    public static long intervalToMillis(SqlIntervalLiteral.IntervalValue interval) {
        return intervalToMillis(
            interval.getIntervalLiteral(),
            interval.getIntervalQualifier());
    }

    public static long intervalToMillis(
        String literal,
        SqlIntervalQualifier intervalQualifier) {
        Preconditions.checkArgument(!intervalQualifier.isYearMonth(),
            "interval must be day time");
        long[] ret;
        try {
            ret = intervalQualifier.evaluateIntervalLiteral(literal,
                intervalQualifier.getParserPosition(), DingoRelDataTypeSystemImpl.DEFAULT);
            assert ret != null;
        } catch (CalciteContextException e) {
            throw new RuntimeException("Error while parsing day-to-second interval " + literal, e);
        }
        long l = 0;
        long[] conv = new long[5];
        conv[4] = 1; // millisecond
        conv[3] = conv[4] * 1000; // second
        conv[2] = conv[3] * 60; // minute
        conv[1] = conv[2] * 60; // hour
        conv[0] = conv[1] * 24; // day
        for (int i = 1; i < ret.length; i++) {
            l += Math.multiplyExact(conv[i - 1] , ret[i]);
        }
        return ret[0] * l;
    }
}
