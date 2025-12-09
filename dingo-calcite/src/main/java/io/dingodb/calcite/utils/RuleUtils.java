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

package io.dingodb.calcite.utils;

import io.dingodb.calcite.type.DingoSqlTypeFactory;
import io.dingodb.common.time.DingoTimeZoneContext;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;

public class RuleUtils {

    public static @Nullable ConditionInfo checkCondition(@NonNull RexNode rexNode) {
        switch (rexNode.getKind()) {
            case LESS_THAN:
                return getConditionInfo((RexCall) rexNode, SqlKind.GREATER_THAN);
            case LESS_THAN_OR_EQUAL:
                return getConditionInfo((RexCall) rexNode, SqlKind.GREATER_THAN_OR_EQUAL);
            case GREATER_THAN:
                return getConditionInfo((RexCall) rexNode, SqlKind.LESS_THAN);
            case GREATER_THAN_OR_EQUAL:
                return getConditionInfo((RexCall) rexNode, SqlKind.LESS_THAN_OR_EQUAL);
            case EQUALS:
                return getConditionInfo((RexCall) rexNode, SqlKind.EQUALS);
            default:
                break;
        }
        return null;
    }

    private static @Nullable ConditionInfo getConditionInfo(@NonNull RexCall rexCall, SqlKind reverseKind) {
        RexNode op0 = rexCall.operands.get(0);
        RexNode op1 = rexCall.operands.get(1);
        ConditionInfo info = new ConditionInfo();
        if (checkConditionOp(op0, op1, info)) {
            info.kind = rexCall.getKind();
        } else if (checkConditionOp(op1, op0, info)) {
            info.kind = reverseKind;
        } else {
            return null;
        }
        return info;
    }

    private static boolean checkConditionOp(@NonNull RexNode op0, RexNode op1, ConditionInfo info) {
        if (op0.getKind() == SqlKind.INPUT_REF) {
            if (op1.getKind() == SqlKind.LITERAL) {
                info.index = ((RexInputRef) op0).getIndex();
                info.value = (RexLiteral) op1;
                return true;
            } else if (op1 instanceof RexCall) {
                RexCall rexCall = (RexCall) op1;
                if (rexCall.op instanceof SqlCastFunction
                    && rexCall.type.getSqlTypeName() == SqlTypeName.DATE
                    && rexCall.getOperands().size() == 1
                    && rexCall.getOperands().get(0).getKind() == SqlKind.LITERAL
                ) {
                    RexLiteral rexLiteral = (RexLiteral) rexCall.getOperands().get(0);
                    if (rexLiteral.getValue() instanceof NlsString) {
                        NlsString val = (NlsString) rexLiteral.getValue();
                        RexBuilder rexBuilder = new RexBuilder(DingoSqlTypeFactory.INSTANCE);
                        Calendar calendar = Calendar.getInstance(DingoTimeZoneContext.getTimeZone());
                        Object dateTime =
                            DingoTimeZoneContext.getProcessor().processDateTime(val.getValue(), DateTimeType.DATE);
                        if (dateTime == null) {
                            return false;
                        }
                        Date date = (Date) dateTime;
                        calendar.setTime(date);
                        DateString dateString = DateString.fromCalendarFields(calendar);
                        info.index = ((RexInputRef) op0).getIndex();
                        info.value = rexBuilder.makeDateLiteral(dateString);
                        return true;
                    }
                } else if (rexCall.op instanceof SqlCastFunction
                    && rexCall.type.getSqlTypeName() == SqlTypeName.TIMESTAMP
                    && rexCall.getOperands().size() == 1
                    && rexCall.getOperands().get(0).getKind() == SqlKind.LITERAL) {
                    RexLiteral rexLiteral = (RexLiteral) rexCall.getOperands().get(0);
                    if (rexLiteral.getValue() instanceof NlsString) {
                        NlsString val = (NlsString) rexLiteral.getValue();
                        RexBuilder rexBuilder = new RexBuilder(DingoSqlTypeFactory.INSTANCE);
                        Object dateTime =
                            DingoTimeZoneContext.getProcessor().processDateTime(val.getValue(), DateTimeType.TIMESTAMP);
                        if (dateTime == null) {
                            return false;
                        }
                        Timestamp timestamp = (Timestamp) dateTime;
                        Calendar calendar = Calendar.getInstance(DingoTimeZoneContext.getTimeZone());
                        calendar.setTimeInMillis(timestamp.getTime());
                        TimestampString timestampString = TimestampString.fromCalendarFields(calendar);
                        info.index = ((RexInputRef) op0).getIndex();
                        info.value = rexBuilder.makeTimestampLiteral(timestampString, 19);
                        return true;
                    }
                } else if (rexCall.op instanceof SqlCastFunction
                    && rexCall.type.getSqlTypeName() == SqlTypeName.TIME
                    && rexCall.getOperands().size() == 1
                    && rexCall.getOperands().get(0).getKind() == SqlKind.LITERAL) {
                    RexLiteral rexLiteral = (RexLiteral) rexCall.getOperands().get(0);
                    if (rexLiteral.getValue() instanceof NlsString) {
                        NlsString val = (NlsString) rexLiteral.getValue();
                        RexBuilder rexBuilder = new RexBuilder(DingoSqlTypeFactory.INSTANCE);
                        try {
                            TimeString timeString = new TimeString(val.getValue());
                            info.value = rexBuilder.makeTimeLiteral(timeString, 8);
                            info.index = ((RexInputRef) op0).getIndex();
                        } catch (Exception e) {
                            return false;
                        }
                        return true;
                    }
                }
            }
        } else if (op0 instanceof RexCall) {
            RexCall rexCall = (RexCall) op0;
            if (rexCall.op instanceof SqlCastFunction
                && rexCall.type.getSqlTypeName() == SqlTypeName.INTEGER
                && rexCall.getOperands().size() == 1
                && rexCall.getOperands().get(0).getKind() == SqlKind.INPUT_REF
                && op1.getKind() == SqlKind.LITERAL
            ) {
                RexInputRef rexInputRef = (RexInputRef) rexCall.getOperands().get(0);
                info.index = rexInputRef.getIndex();
                info.value = (RexLiteral) op1;
                return true;
            }
        }
        return false;
    }

    public static class ConditionInfo {
        public SqlKind kind;
        public int index;
        public RexLiteral value;
    }
}
