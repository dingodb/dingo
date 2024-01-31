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

package io.dingodb.calcite.visitor;

import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.RexLiteralUtils;
import io.dingodb.exec.expr.SqlExprCompileContext;
import io.dingodb.exec.fun.DingoFunFactory;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.compiler.CastingFactory;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.OpExpr;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.NullaryOp;
import io.dingodb.expr.runtime.op.TertiaryOp;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.op.VariadicOp;
import io.dingodb.expr.runtime.op.time.DateFormat2FunFactory;
import io.dingodb.expr.runtime.op.time.TimeFormat2FunFactory;
import io.dingodb.expr.runtime.op.time.TimestampFormat2FunFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class RexConverter implements RexVisitor<@NonNull Expr> {
    private static final RexConverter INSTANCE = new RexConverter();

    private final DingoFunFactory funFactory;

    private RexConverter() {
        funFactory = DingoFunFactory.getInstance();
    }

    public static @NonNull Expr convert(@NonNull RexNode rexNode) {
        return rexNode.accept(INSTANCE);
    }

    public static @NonNull Expr var(int index) {
        return Exprs.op(Exprs.INDEX, Exprs.var(SqlExprCompileContext.TUPLE_VAR_NAME), Exprs.val(index));
    }

    @Override
    public @NonNull Expr visitInputRef(@NonNull RexInputRef inputRef) {
        return var(inputRef.getIndex());
    }

    @Override
    public @NonNull Expr visitLocalRef(@NonNull RexLocalRef localRef) {
        throw new UnsupportedRexNode(localRef);
    }

    @Override
    public @NonNull Expr visitLiteral(@NonNull RexLiteral literal) {
        Object value = RexLiteralUtils.convertFromRexLiteral(literal);
        return Exprs.val(value, DefinitionMapper.mapToDingoType(literal.getType()).getType());
    }

    @Override
    public @NonNull Expr visitCall(@NonNull RexCall call) {
        SqlKind kind = call.getKind();
        switch (kind) {
            case PLUS_PREFIX:
                return Exprs.op(
                    Exprs.POS,
                    call.getOperands().get(0).accept(this)
                );
            case MINUS_PREFIX:
                return Exprs.op(
                    Exprs.NEG,
                    call.getOperands().get(0).accept(this)
                );
            case PLUS:
                return Exprs.op(
                    Exprs.ADD,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case MINUS:
                return Exprs.op(
                    Exprs.SUB,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case TIMES:
                return Exprs.op(
                    Exprs.MUL,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case DIVIDE:
                return Exprs.op(
                    Exprs.DIV,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case LESS_THAN:
                return Exprs.op(
                    Exprs.LT,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case LESS_THAN_OR_EQUAL:
                return Exprs.op(
                    Exprs.LE,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case EQUALS:
                return Exprs.op(
                    Exprs.EQ,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case GREATER_THAN:
                return Exprs.op(
                    Exprs.GT,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case GREATER_THAN_OR_EQUAL:
                return Exprs.op(
                    Exprs.GE,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case NOT_EQUALS:
                return Exprs.op(
                    Exprs.NE,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case NOT:
                return Exprs.op(
                    Exprs.NOT,
                    call.getOperands().get(0).accept(this)
                );
            case AND:
                return Exprs.op(
                    Exprs.AND_FUN,
                    getOperandsArray(call)
                );
            case OR:
                return Exprs.op(
                    Exprs.OR_FUN,
                    getOperandsArray(call)
                );
            case CASE:
                return Exprs.op(
                    Exprs.CASE,
                    getOperandsArray(call)
                );
            case IS_NULL:
                return Exprs.op(
                    Exprs.IS_NULL,
                    call.getOperands().get(0).accept(this)
                );
            case IS_NOT_NULL:
                return Exprs.op(Exprs.NOT, Exprs.op(
                    Exprs.IS_NULL,
                    call.getOperands().get(0).accept(this)
                ));
            case IS_TRUE:
                return Exprs.op(
                    Exprs.IS_TRUE,
                    call.getOperands().get(0).accept(this)
                );
            case IS_NOT_TRUE:
                return Exprs.op(Exprs.NOT, Exprs.op(
                    Exprs.IS_TRUE,
                    call.getOperands().get(0).accept(this)
                ));
            case IS_FALSE:
                return Exprs.op(
                    Exprs.IS_FALSE,
                    call.getOperands().get(0).accept(this)
                );
            case IS_NOT_FALSE:
                return Exprs.op(Exprs.NOT, Exprs.op(
                    Exprs.IS_FALSE,
                    call.getOperands().get(0).accept(this)
                ));
            case TRIM:
                RexNode r = call.getOperands().get(0);
                if (r.getKind() == SqlKind.LITERAL && ((RexLiteral) r).getTypeName() == SqlTypeName.SYMBOL) {
                    SqlTrimFunction.Flag flag = (SqlTrimFunction.Flag) ((RexLiteral) r).getValue();
                    if (flag != null) {
                        switch (flag) {
                            case BOTH:
                                return Exprs.op(
                                    Exprs.TRIM2,
                                    call.getOperands().get(2).accept(this),
                                    call.getOperands().get(1).accept(this)
                                );
                            case LEADING:
                                return Exprs.op(
                                    Exprs.LTRIM2,
                                    call.getOperands().get(2).accept(this),
                                    call.getOperands().get(1).accept(this)
                                );
                            case TRAILING:
                                return Exprs.op(
                                    Exprs.RTRIM2,
                                    call.getOperands().get(2).accept(this),
                                    call.getOperands().get(1).accept(this)
                                );
                        }
                    }
                }
                break;
            case LTRIM:
            case RTRIM:
                // These are Oracle functions.
                break;
            case CAST:
                RelDataType type = call.getType();
                return Exprs.op(
                    CastingFactory.get(DefinitionMapper.mapToDingoType(type).getType(), ExprConfig.ADVANCED),
                    call.getOperands().get(0).accept(this)
                );
            case ARRAY_VALUE_CONSTRUCTOR:
            case MULTISET_VALUE_CONSTRUCTOR:
                return Exprs.op(
                    Exprs.LIST,
                    getOperandsArray(call)
                );
            case ITEM:
                RexNode value = call.getOperands().get(0);
                RexNode index = call.getOperands().get(1);
                if ((value.getType().getSqlTypeName() == SqlTypeName.ARRAY
                    || value.getType().getSqlTypeName() == SqlTypeName.MULTISET)
                    && index.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
                    return Exprs.op(Exprs.INDEX, value.accept(this), Exprs.op(Exprs.SUB, index.accept(this), 1));
                } else {
                    return Exprs.op(Exprs.INDEX, value.accept(this), index.accept(this));
                }
            case MAP_VALUE_CONSTRUCTOR:
                return Exprs.op(Exprs.MAP, getOperandsArray(call));
            case LIKE:
                Expr pattern;
                if (call.getOperands().size() == 3) {
                    pattern = Exprs.op(
                        Exprs._CP2,
                        call.getOperands().get(1).accept(this),
                        call.getOperands().get(2).accept(this)
                    );
                } else {
                    pattern = Exprs.op(Exprs._CP1, call.getOperands().get(1).accept(this));
                }
                if (call.getOperands().get(1).getType().getSqlTypeName().equals(SqlTypeName.BINARY)) {
                    return Exprs.op(
                        Exprs.MATCHES,
                        call.getOperands().get(0).accept(this),
                        pattern
                    );
                } else {
                    return Exprs.op(
                        Exprs.MATCHES_NC,
                        call.getOperands().get(0).accept(this),
                        pattern
                    );
                }
            case OTHER:
                if (call.op.equals(SqlStdOperatorTable.CONCAT)) {
                    return Exprs.op(
                        Exprs.CONCAT,
                        call.getOperands().get(0).accept(this),
                        call.getOperands().get(1).accept(this)
                    );
                } else if (call.op.equals(SqlStdOperatorTable.SLICE)) {
                    return Exprs.op(
                        Exprs.SLICE,
                        call.getOperands().get(0).accept(this),
                        0
                    );
                } else {
                    OpExpr opExpr = getFunFromFactory(call);
                    if (opExpr != null) {
                        return opExpr;
                    }
                }
                break;
            case MOD:
                return Exprs.op(
                    Exprs.MOD,
                    call.getOperands().get(0).accept(this),
                    call.getOperands().get(1).accept(this)
                );
            case FLOOR:
                return Exprs.op(
                    Exprs.FLOOR,
                    call.getOperands().get(0).accept(this)
                );
            case CEIL:
                return Exprs.op(
                    Exprs.CEIL,
                    call.getOperands().get(0).accept(this)
                );
            case OTHER_FUNCTION: {
                OpExpr opExpr = getFunFromFactory(call);
                if (opExpr != null) {
                    return opExpr;
                }
                break;
            }
            default:
                break;
        }
        throw new UnsupportedRexNode(call);
    }

    @Override
    public @NonNull Expr visitOver(RexOver over) {
        throw new UnsupportedRexNode(over);
    }

    @Override
    public @NonNull Expr visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedRexNode(correlVariable);
    }

    @Override
    public @NonNull Expr visitDynamicParam(@NonNull RexDynamicParam dynamicParam) {
        return Exprs.op(Exprs.INDEX, Exprs.var(SqlExprCompileContext.SQL_DYNAMIC_VAR_NAME), dynamicParam.getIndex());
    }

    @Override
    public @NonNull Expr visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedRexNode(rangeRef);
    }

    @Override
    public @NonNull Expr visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new UnsupportedRexNode(fieldAccess);
    }

    @Override
    public @NonNull Expr visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedRexNode(subQuery);
    }

    @Override
    public @NonNull Expr visitTableInputRef(RexTableInputRef fieldRef) {
        throw new UnsupportedRexNode(fieldRef);
    }

    @Override
    public @NonNull Expr visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        throw new UnsupportedRexNode(fieldRef);
    }

    private @Nullable OpExpr getFunFromFactory(@NonNull RexCall call) {
        String funName = call.op.getName();
        switch (call.getOperands().size()) {
            case 0:
                NullaryOp nullaryOp = funFactory.getNullaryFun(funName);
                if (nullaryOp != null) {
                    return Exprs.op(nullaryOp);
                }
                break;
            case 1:
                UnaryOp unaryOp = funFactory.getUnaryFun(funName);
                if (unaryOp != null) {
                    return Exprs.op(
                        unaryOp,
                        call.getOperands().get(0).accept(this)
                    );
                }
                break;
            case 2:
                // TODO: This should not exists.
                if (funName.equals("LIKE_BINARY")) {
                    return Exprs.op(
                        Exprs.MATCHES,
                        call.getOperands().get(0).accept(this),
                        Exprs.op(Exprs._CP1, call.getOperands().get(1).accept(this))
                    );
                }
                BinaryOp binaryOp = funFactory.getBinaryFun(funName);
                if (binaryOp != null) {
                    switch (binaryOp.getName()) {
                        case DateFormat2FunFactory.NAME:
                        case TimeFormat2FunFactory.NAME:
                        case TimestampFormat2FunFactory.NAME:
                            return Exprs.op(
                                binaryOp,
                                call.getOperands().get(0).accept(this),
                                Exprs.op(Exprs._CTF, call.getOperands().get(1).accept(this))
                            );
                        default:
                            return Exprs.op(
                                binaryOp,
                                call.getOperands().get(0).accept(this),
                                call.getOperands().get(1).accept(this)
                            );
                    }
                }
                break;
            case 3:
                TertiaryOp tertiaryOp = funFactory.getTertiaryFun(funName);
                if (tertiaryOp != null) {
                    return Exprs.op(
                        tertiaryOp,
                        call.getOperands().get(0).accept(this),
                        call.getOperands().get(1).accept(this),
                        call.getOperands().get(2).accept(this)
                    );
                }
                break;
            default:
                break;
        }
        VariadicOp variadicOp = funFactory.getVariadicFun(funName);
        if (variadicOp != null) {
            return Exprs.op(
                variadicOp,
                getOperandsArray(call)
            );
        }
        return null;
    }

    @NonNull
    private Object @NonNull [] getOperandsArray(@NonNull RexCall call) {
        return call.getOperands().stream()
            .map(o -> o.accept(this))
            .toArray(Expr[]::new);
    }

    public static class UnsupportedRexNode extends UnsupportedOperationException {
        private static final long serialVersionUID = 4743457403279368767L;

        public UnsupportedRexNode(@NonNull RexNode rexNode) {
            super("RexNode \"" + rexNode + "\" is not supported.");
        }
    }
}
