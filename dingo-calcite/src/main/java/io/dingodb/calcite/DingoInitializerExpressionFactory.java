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

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.GregorianCalendar;


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

        /**
         * if default value is function, we need to call function to get value
         */
        if (defaultValue instanceof SqlIdentifier || defaultValue instanceof SqlBasicCall) {
            defaultValue = getDefaultValueWhenUsingFunc(defaultValue, type);
        }

        if (defaultValue != null) {
            return context.getRexBuilder().makeLiteral(defaultValue, type);
        }
        return super.newColumnDefaultValue(table, column, context);
    }

    private Object getDefaultValueWhenUsingFunc(final Object inputValue, RelDataType type) {
        String methodName = "";
        if (inputValue instanceof SqlIdentifier) {
            methodName = ((SqlIdentifier) inputValue).getSimple();
        } else if (inputValue instanceof SqlBasicCall) {
            methodName = ((SqlBasicCall) inputValue).getOperator().getName();
        }

        if (methodName.length() == 0) {
            return inputValue;
        }

        Object defaultValue = inputValue;
        Method method = DingoFunctions.getInstance().getDingoFunction(methodName);
        if (method != null) {
            try {
                defaultValue = method.invoke(null);
                defaultValue = convertDefaultValueWhenUsingFunc(defaultValue, type);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                log.error("Error when invoke function {}", methodName, e.getCause(), e);
                defaultValue = null;
            }
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
                    TimeString timeString = new TimeString(inputValue.toString());
                    defaultValue = timeString;
                }
                break;
            }
            case TIMESTAMP: {
                if (inputValue instanceof java.sql.Timestamp) {
                    TimestampString timeString = new TimestampString(inputValue.toString());
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
