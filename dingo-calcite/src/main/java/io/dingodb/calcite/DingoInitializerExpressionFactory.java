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

import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.DingoExprCompileException;
import io.dingodb.expr.parser.exception.DingoExprParseException;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import io.dingodb.expr.parser.var.Var;
import io.dingodb.expr.runtime.RtConst;
import io.dingodb.expr.runtime.RtNull;
import io.dingodb.expr.runtime.op.time.utils.DingoDateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;


@Slf4j
class DingoInitializerExpressionFactory extends NullInitializerExpressionFactory {
    static DingoInitializerExpressionFactory INSTANCE = new DingoInitializerExpressionFactory();

    private DingoInitializerExpressionFactory() {
    }

    @Override
    public ColumnStrategy generationStrategy(RelOptTable table, int column) {
        DingoTable dingoTable = DingoTable.dingo(table);
        return dingoTable.getTableDefinition().getColumnStrategy(column);
    }

    @Override
    public RexNode newColumnDefaultValue(RelOptTable table, int column, InitializerContext context) {
        DingoTable dingoTable = DingoTable.dingo(table);
        Object defaultValue = dingoTable.getTableDefinition().getColumn(column).getDefaultValue();
        RelDataType type = table.getRowType().getFieldList().get(column).getType();
        if (defaultValue == null) {
            return super.newColumnDefaultValue(table, column, context);
        }

        /**
         * if default value is function, we need to call function to get value
         */
        try {
            /**
             * discard the special character "`" imported by MySQL dialect.
             */
            defaultValue = defaultValue.toString().trim().replace('`', ' ').trim();
            Expr expr = DingoExprCompiler.parse(defaultValue.toString());
            if (expr instanceof Var) {
                expr = DingoExprCompiler.parse(defaultValue.toString().trim() + "()");
            }
            defaultValue = getDefaultValueByExpression(expr, type);
            if (defaultValue != null) {
                return context.getRexBuilder().makeLiteral(defaultValue, type);
            }
        } catch (DingoExprParseException e) {
            log.error("parse default value error:{}", e.toString(), e);
        }
        return super.newColumnDefaultValue(table, column, context);
    }

    private Object getDefaultValueByExpression(final Expr inputExpr, RelDataType type) {
        Object defaultValue = null;

        try {
            defaultValue = inputExpr.compileIn(null);
            if (defaultValue instanceof RtNull) {
                return null;
            }

            RtConst valueOfConst = (RtConst) defaultValue;
            Object constValue = valueOfConst.getValue();
            switch (type.getSqlTypeName()) {
                case DATE:
                    java.sql.Date inputValue = null;
                    try {
                        if (constValue instanceof  java.sql.Date) {
                            inputValue = (java.sql.Date) constValue;
                        } else if (constValue instanceof java.lang.String) {
                            inputValue = java.sql.Date.valueOf(constValue.toString());
                        }
                        defaultValue = new DateString(inputValue.toString());
                    } catch (Exception ex) {
                        log.error("Set default value:{} of Date catch exception:{}",
                            constValue.toString(), ex.toString(), ex);
                        throw new RuntimeException("Invalid Input DateFormat, Expect(yyyy-MM-dd)");
                    }
                    break;
                case TIME:
                    java.sql.Time time = null;
                    try {
                        if (constValue instanceof java.sql.Time) {
                            time = (java.sql.Time) valueOfConst.getValue();
                        } else if (constValue instanceof java.lang.String) {
                            time = java.sql.Time.valueOf(constValue.toString());
                        }
                        long offsetInMillis = DingoDateTimeUtils.getLocalZoneOffset().getTotalSeconds() * 1000;
                        Time newTimeWithOffset = new Time(time.getTime() - offsetInMillis);
                        defaultValue = new TimeString(newTimeWithOffset.toString());
                    } catch (Exception ex) {
                        log.error("Set default value:{} of Time catch exception:{}",
                            constValue.toString(), ex.toString(), ex);
                        throw new RuntimeException("Invalid Input TimeFormat: Expect(HH:mm:ss)");
                    }
                    break;
                case TIMESTAMP:
                    try {
                        java.sql.Timestamp timestamp = null;
                        if (constValue instanceof java.sql.Timestamp) {
                            timestamp = (java.sql.Timestamp) valueOfConst.getValue();
                        } else if (constValue instanceof java.lang.String) {
                            timestamp = java.sql.Timestamp.valueOf(constValue.toString());
                        }
                        defaultValue = timestamp
                            .toLocalDateTime()
                            .toEpochSecond(DingoDateTimeUtils.getLocalZoneOffset()) * 1000;
                    } catch (Exception ex) {
                        log.error("Set default value:{} of TimeStamp catch exception:{}",
                            constValue.toString(), ex.toString(), ex);
                        throw new RuntimeException("Invalid Input TimeStampFormat: Expect(yyyy-MM-dd HH:mm:dd)");
                    }
                    break;
                default:
                    defaultValue = valueOfConst.getValue();
                    break;
            }
        } catch (DingoExprCompileException ex) {
            log.error("compile expr error:{}", ex.toString(), ex);
        }
        return defaultValue;
    }

    private Object convertDefaultValueWhenUsingFunc(final Object inputValue, RelDataType type) {
        Object defaultValue = inputValue;
        switch (type.getSqlTypeName()) {
            case DATE: {
                /**
                 * current_date will return java.sql.Date, we need to convert to Calendar
                 * RexBuilder.java: clean method will use this Calender
                 */
                if (inputValue instanceof java.sql.Date) {
                    DateString timeString = new DateString(inputValue.toString());
                    defaultValue = timeString;
                    break;
                }
                break;
            }
            case TIME: {
                if (inputValue instanceof java.sql.Time) {
                    Long unixTime = ((java.sql.Time) inputValue).getTime();
                    unixTime -= TimeZone.getDefault().getRawOffset();
                    Time time = new Time(unixTime);
                    TimeString timeString = new TimeString(time.toString());
                    defaultValue = timeString;
                }
                break;
            }
            case TIMESTAMP: {
                if (inputValue instanceof java.sql.Timestamp) {
                    Timestamp timeStamp = (Timestamp) inputValue;
                    Long realTime = timeStamp.getTime();
                    TimestampString timeString = TimestampString.fromMillisSinceEpoch(realTime);
                    defaultValue = timeString;
                }
                break;
            }

            default: {
                break;
            }
        }
        return defaultValue;
    }

}
