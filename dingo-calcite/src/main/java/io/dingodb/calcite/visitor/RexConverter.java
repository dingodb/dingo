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

import io.dingodb.calcite.utils.RexLiteralUtils;
import io.dingodb.exec.expr.SqlExprCompileContext;
import io.dingodb.exec.fun.DingoFunFactory;
import io.dingodb.exec.fun.special.CastListItemsOp;
import io.dingodb.exec.fun.special.ListConstructorOp;
import io.dingodb.exec.fun.special.MapConstructorOp;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.core.evaluator.EvaluatorFactory;
import io.dingodb.expr.core.evaluator.EvaluatorKey;
import io.dingodb.expr.parser.DingoExprParser;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.FunFactory;
import io.dingodb.expr.parser.OpFactory;
import io.dingodb.expr.parser.exception.UndefinedFunctionName;
import io.dingodb.expr.parser.op.IndexOp;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.parser.op.OpWithEvaluator;
import io.dingodb.expr.parser.value.Null;
import io.dingodb.expr.parser.value.Value;
import io.dingodb.expr.parser.var.Var;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class RexConverter extends RexVisitorImpl<Expr> {
    private static final RexConverter INSTANCE = new RexConverter();

    private static final List<String> REX_CONST_LITERAL = Stream
        .of("BOTH", "LEADING", "TRAILING")
        .collect(Collectors.toList());

    private final FunFactory funFactory;

    private RexConverter() {
        super(true);
        funFactory = DingoFunFactory.getInstance();
    }

    private static int typeCodeOf(@NonNull RelDataType type) {
        return TypeCode.codeOf(type.getSqlTypeName().getName());
    }

    private static Op getCastOpWithCheck(RelDataType type, RelDataType inputType) {
        Op op;
        try {
            op = DingoFunFactory.getInstance().getCastFun(typeCodeOf(type));
            if (op instanceof OpWithEvaluator) { // Check if the evaluator exists.
                EvaluatorFactory factory = ((OpWithEvaluator) op).getFactory();
                factory.getEvaluator(EvaluatorKey.of(typeCodeOf(inputType)));
            }
        } catch (UndefinedFunctionName e) {
            throw new UnsupportedOperationException(
                "Unsupported cast operation: from \"" + inputType + "\" to \"" + type + "\"."
            );
        }
        return op;
    }

    public static Expr convert(@NonNull RexNode rexNode) {
        return rexNode.accept(INSTANCE);
    }

    @Override
    public @NonNull Expr visitInputRef(@NonNull RexInputRef inputRef) {
        IndexOp op = new IndexOp();
        op.setExprArray(new Expr[]{
            new Var(SqlExprCompileContext.SQL_TUPLE_VAR_NAME),
            Value.of(inputRef.getIndex())
        });
        return op;
    }

    @Override
    public Expr visitLiteral(@NonNull RexLiteral literal) {
        Object value;
        if (literal.getType().getSqlTypeName() == SqlTypeName.SYMBOL) {
            // TODO: should consider the symbol enum, not the string, to avoid misunderstand from a real string.
            value = Objects.requireNonNull(literal.getValue()).toString();
        } else {
            value = RexLiteralUtils.convertFromRexLiteral(literal);
        }
        // `null` is implemented by Var in dingo-expr.
        return value != null ? Value.of(value) : Null.INSTANCE;
    }

    @Override
    public @NonNull Expr visitCall(@NonNull RexCall call) {
        Op op;
        SqlKind kind = call.getKind();
        switch (kind) {
            case PLUS_PREFIX:
                op = OpFactory.getUnary(DingoExprParser.ADD);
                break;
            case MINUS_PREFIX:
                op = OpFactory.getUnary(DingoExprParser.SUB);
                break;
            case PLUS:
                op = OpFactory.getBinary(DingoExprParser.ADD);
                break;
            case MINUS:
                op = OpFactory.getBinary(DingoExprParser.SUB);
                break;
            case TIMES:
                op = OpFactory.getBinary(DingoExprParser.MUL);
                break;
            case DIVIDE:
                op = OpFactory.getBinary(DingoExprParser.DIV);
                break;
            case LESS_THAN:
                op = OpFactory.getBinary(DingoExprParser.LT);
                break;
            case LESS_THAN_OR_EQUAL:
                op = OpFactory.getBinary(DingoExprParser.LE);
                break;
            case EQUALS:
                op = OpFactory.getBinary(DingoExprParser.EQ);
                break;
            case GREATER_THAN:
                op = OpFactory.getBinary(DingoExprParser.GT);
                break;
            case GREATER_THAN_OR_EQUAL:
                op = OpFactory.getBinary(DingoExprParser.GE);
                break;
            case NOT_EQUALS:
                op = OpFactory.getBinary(DingoExprParser.NE);
                break;
            case NOT:
                op = OpFactory.getUnary(DingoExprParser.NOT);
                break;
            case AND:
            case OR:
            case CASE:
            case IS_NULL:
            case IS_NOT_NULL:
            case IS_TRUE:
            case IS_NOT_TRUE:
            case IS_FALSE:
            case IS_NOT_FALSE:
            case TRIM:
            case LTRIM:
            case RTRIM:
                op = funFactory.getFun(kind.name());
                break;
            case CAST:
                RelDataType type = call.getType();
                RexNode operand = call.getOperands().get(0);
                SqlTypeName sqlTypeName = type.getSqlTypeName();
                if (sqlTypeName == SqlTypeName.ARRAY || sqlTypeName == SqlTypeName.MULTISET) {
                    op = funFactory.getFun(CastListItemsOp.NAME);
                    RelDataType newType = type.getComponentType();
                    op.setExprArray(new Expr[]{
                        new Value<>(Objects.requireNonNull(newType).getSqlTypeName().getName()),
                        operand.accept(this)
                    });
                    return op;
                }
                op = getCastOpWithCheck(type, operand.getType());
                break;
            case ARRAY_VALUE_CONSTRUCTOR:
            case MULTISET_VALUE_CONSTRUCTOR:
                op = funFactory.getFun(ListConstructorOp.NAME);
                break;
            case ITEM:
                op = funFactory.getFun(DingoFunFactory.ITEM);
                break;
            case MAP_VALUE_CONSTRUCTOR:
                op = funFactory.getFun(MapConstructorOp.NAME);
                break;
            case LIKE:
            case OTHER:
                if (call.op.getName().equals("||")) {
                    op = funFactory.getFun("concat");
                } else {
                    op = funFactory.getFun(call.op.getName());
                }
                break;
            case MOD:
                op = funFactory.getFun(call.op.getName());
                break;
            case FLOOR:
            case CEIL:
            case OTHER_FUNCTION: {
                op = funFactory.getFun(call.op.getName());
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported operation: \"" + call + "\".");
        }
        List<Expr> exprList = new ArrayList<>();
        for (RexNode node : call.getOperands()) {
            Expr expr = node.accept(this);
            if (REX_CONST_LITERAL.contains(expr.toString())) {
                exprList.add(new Value<String>(expr.toString()));
            } else {
                exprList.add(expr);
            }
        }
        op.setExprArray(exprList.toArray(new Expr[0]));
        return op;
    }

    @Override
    public @NonNull Expr visitDynamicParam(@NonNull RexDynamicParam dynamicParam) {
        IndexOp op = new IndexOp();
        op.setExprArray(new Expr[]{
            new Var(SqlExprCompileContext.SQL_DYNAMIC_VAR_NAME),
            Value.of(dynamicParam.getIndex())
        });
        return op;
    }
}
