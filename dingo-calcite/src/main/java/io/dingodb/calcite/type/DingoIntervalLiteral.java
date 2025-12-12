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

package io.dingodb.calcite.type;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.DingoAnsiSqlDialect;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.DingoParserUtils;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Calendar;

import static java.util.Objects.requireNonNull;

public class DingoIntervalLiteral extends SqlIntervalLiteral {

    protected DingoIntervalLiteral(int sign,
                                   String intervalStr,
                                   SqlIntervalQualifier intervalQualifier,
                                   SqlTypeName sqlTypeName,
                                   SqlParserPos pos) {
        super(sign, intervalStr, intervalQualifier, sqlTypeName, pos);
    }

    @Override
    public RelDataType createSqlType(RelDataTypeFactory typeFactory) {
        BitString bitString;
        switch (getTypeName()) {
            case NULL:
            case BOOLEAN:
                RelDataType ret = typeFactory.createSqlType(getTypeName());
                ret = typeFactory.createTypeWithNullability(ret, null == value);
                return ret;
            case BINARY:
                bitString = (BitString) requireNonNull(value, "value");
                int bitCount = bitString.getBitCount();
                return typeFactory.createSqlType(SqlTypeName.BINARY, bitCount / 8);
            case CHAR:
                NlsString string = (NlsString) requireNonNull(value, "value");
                Charset charset = string.getCharset();
                if (null == charset) {
                    charset = typeFactory.getDefaultCharset();
                }
                SqlCollation collation = string.getCollation();
                if (null == collation) {
                    collation = SqlCollation.COERCIBLE;
                }
                RelDataType type =
                    typeFactory.createSqlType(
                        SqlTypeName.CHAR,
                        string.getValue().length());
                type =
                    typeFactory.createTypeWithCharsetAndCollation(
                        type,
                        charset,
                        collation);
                return type;

            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                SqlIntervalLiteral.IntervalValue intervalValue =
                    (SqlIntervalLiteral.IntervalValue) requireNonNull(value, "value");
                return typeFactory.createSqlIntervalType(
                    intervalValue.getIntervalQualifier());

            case SYMBOL:
                return typeFactory.createSqlType(SqlTypeName.SYMBOL);

            case INTEGER: // handled in derived class
            case TIME: // handled in derived class
            case VARCHAR: // should never happen
            case VARBINARY: // should never happen

            default:
                throw Util.needToImplement(toString() + ", operand=" + value);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        DingoAnsiSqlDialect.DEFAULT.unparseSqlIntervalLiteral(writer, this, leftPrec, rightPrec);
    }

    @Override
    public <T> T getValueAs(Class<T> clazz) {
        Object value = this.value;
        if (clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        if (getTypeName() == SqlTypeName.NULL) {
            return clazz.cast(NullSentinel.INSTANCE);
        }
        requireNonNull(value, "value");
        final SqlIntervalQualifier qualifier;
        switch (getTypeName()) {
            case CHAR:
                if (clazz == String.class) {
                    return clazz.cast(((NlsString) value).getValue());
                }
                break;
            case BINARY:
                if (clazz == byte[].class) {
                    return clazz.cast(((BitString) value).getAsByteArray());
                }
                break;
            case DECIMAL:
                if (clazz == Long.class) {
                    return clazz.cast(((BigDecimal) value).longValueExact());
                }
                // fall through
            case BIGINT:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
            case DOUBLE:
            case REAL:
            case FLOAT:
                if (clazz == Long.class) {
                    return clazz.cast(((BigDecimal) value).longValueExact());
                } else if (clazz == Integer.class) {
                    return clazz.cast(((BigDecimal) value).intValueExact());
                } else if (clazz == Short.class) {
                    return clazz.cast(((BigDecimal) value).shortValueExact());
                } else if (clazz == Byte.class) {
                    return clazz.cast(((BigDecimal) value).byteValueExact());
                } else if (clazz == Double.class) {
                    return clazz.cast(((BigDecimal) value).doubleValue());
                } else if (clazz == Float.class) {
                    return clazz.cast(((BigDecimal) value).floatValue());
                }
                break;
            case DATE:
                if (clazz == Calendar.class) {
                    return clazz.cast(((DateString) value).toCalendar());
                }
                break;
            case TIME:
                if (clazz == Calendar.class) {
                    return clazz.cast(((TimeString) value).toCalendar());
                }
                break;
            case TIMESTAMP:
                if (clazz == Calendar.class) {
                    return clazz.cast(((TimestampString) value).toCalendar());
                }
                break;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                final SqlIntervalLiteral.IntervalValue valMonth =
                    (SqlIntervalLiteral.IntervalValue) value;
                qualifier = valMonth.getIntervalQualifier();
                if (clazz == Long.class) {
                    return clazz.cast(DingoParserUtils.intervalToMonths(valMonth));
                } else if (clazz == BigDecimal.class) {
                    return clazz.cast(BigDecimal.valueOf(getValueAs(Long.class)));
                } else if (clazz == TimeUnitRange.class) {
                    return clazz.cast(qualifier.timeUnitRange);
                } else if (clazz == TimeUnit.class) {
                    return clazz.cast(qualifier.timeUnitRange.startUnit);
                } else if (clazz == SqlIntervalQualifier.class) {
                    return clazz.cast(qualifier);
                }
                break;
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                final SqlIntervalLiteral.IntervalValue valTime =
                    (SqlIntervalLiteral.IntervalValue) value;
                qualifier = valTime.getIntervalQualifier();
                if (clazz == Long.class) {
                    if (valTime.getIntervalLiteral() == null) {
                        return null;
                    }
                    return clazz.cast(DingoParserUtils.intervalToMillis(valTime));
                } else if (clazz == BigDecimal.class) {
                    Long valueAs = getValueAs(Long.class);
                    if (valueAs == null) {
                        return null;
                    }
                    return clazz.cast(BigDecimal.valueOf(valueAs));
                } else if (clazz == TimeUnitRange.class) {
                    return clazz.cast(qualifier.timeUnitRange);
                } else if (clazz == TimeUnit.class) {
                    return clazz.cast(qualifier.timeUnitRange.startUnit);
                } else if (clazz == SqlIntervalQualifier.class) {
                    return clazz.cast(qualifier);
                }
                break;
            default:
                break;
        }
        throw new AssertionError("cannot cast " + value + " as " + clazz);
    }
}
