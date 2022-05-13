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

package io.dingodb.calcite;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class DataUtils {
    private DataUtils() {
    }

    public static Object toCalcite(Object value, @Nonnull SqlTypeName type) {
        switch (type) {
            case DATE:
                if (value instanceof Date) {
                    return new DateString(value.toString());
                }
                break;
            case TIME:
                if (value instanceof Time) {
                    TimeString ts =  new TimeString(value.toString());
                    return ts;
                }
                break;
            case TIMESTAMP:
                if (value instanceof Timestamp) {
                    Timestamp tsConverted = (Timestamp) value;
                    TimestampString ts =  TimestampString.fromMillisSinceEpoch(tsConverted.getTime());
                    return ts;
                }
                break;
            default:
                break;
        }
        return value;
    }

    @Nullable
    public static Object fromRexLiteral(@Nonnull RexLiteral literal) {
        Object value = literal.getValue();
        if (value == null) {
            return null;
        }
        switch (literal.getType().getSqlTypeName()) {
            case CHAR:
            case VARCHAR:
                return ((NlsString) value).getValue();
            case INTEGER:
            case TINYINT:
            case SMALLINT:
                if (value instanceof BigDecimal) {
                    return ((BigDecimal) value).setScale(0, RoundingMode.HALF_UP).intValue();
                }
                return ((Number) value).intValue();
            case BIGINT:
                if (value instanceof BigDecimal) {
                    return ((BigDecimal) value).toBigInteger();
                }
                return ((Number) value).longValue();
            case DATE:
                return new Date(((Calendar) value).getTimeInMillis());
            case TIME:
                return new Time(((Calendar) value).getTimeInMillis());
            case TIMESTAMP:
                return new Timestamp(((Calendar) value).getTimeInMillis());
            default:
                break;
        }
        return value;
    }
}
