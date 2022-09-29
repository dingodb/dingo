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

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.converter.ExprConverter;
import io.dingodb.common.type.converter.RexLiteralConverter;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.expr.SqlExprCompileContext;
import io.dingodb.exec.expr.SqlExprEvalContext;
import io.dingodb.expr.parser.DingoExprParser;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.exception.DingoExprCompileException;
import io.dingodb.expr.parser.exception.UndefinedFunctionName;
import io.dingodb.expr.parser.op.FunFactory;
import io.dingodb.expr.parser.op.IndexOp;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.parser.op.OpFactory;
import io.dingodb.expr.parser.op.OpWithEvaluator;
import io.dingodb.expr.parser.op.SqlCastListItemsOp;
import io.dingodb.expr.parser.value.Null;
import io.dingodb.expr.parser.value.Value;
import io.dingodb.expr.parser.var.Var;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorKey;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class RexConverter extends RexVisitorImpl<Expr> {
    private static final RexConverter INSTANCE = new RexConverter();

    private static final List<String> REX_CONST_LITERAL = Stream
        .of("BOTH", "LEADING", "TRAILING")
        .collect(Collectors.toList());

    private RexConverter() {
        super(true);
    }

    @Nullable
    public static Object convertFromRexLiteral(@Nonnull RexLiteral rexLiteral, @Nonnull DingoType type) {
        if (!rexLiteral.isNull()) {
            // `rexLiteral.getType()` is not always the required type.
            return type.convertFrom(rexLiteral.getValue(), RexLiteralConverter.INSTANCE);
        }
        return null;
    }

    public static Object convertFromRexLiteral(@Nonnull RexLiteral rexLiteral) {
        DingoType type = DingoTypeFactory.fromRelDataType(rexLiteral.getType());
        return convertFromRexLiteral(rexLiteral, type);
    }

    @Nonnull
    public static Object[] convertFromRexLiteralList(@Nonnull List<RexLiteral> values, @Nonnull DingoType type) {
        return IntStream.range(0, values.size())
            .mapToObj(i -> convertFromRexLiteral(values.get(i), Objects.requireNonNull(type.getChild(i))))
            .toArray(Object[]::new);
    }

    public static Expr convert(@Nonnull RexNode rexNode) {
        return rexNode.accept(INSTANCE);
    }

    @Nonnull
    public static SqlExpr toRtExprWithType(@Nonnull RexNode rexNode) {
        return toRtExprWithType(rexNode, rexNode.getType());
    }

    @Nonnull
    public static SqlExpr toRtExprWithType(@Nonnull RexNode rexNode, RelDataType type) {
        return new SqlExpr(
            convert(rexNode).toString(),
            DingoTypeFactory.fromRelDataType(type)
        );
    }

    @Nonnull
    public static List<SqlExpr> toRtExprWithType(@Nonnull List<RexNode> rexNodes, RelDataType type) {
        return IntStream.range(0, rexNodes.size())
            .mapToObj(i -> toRtExprWithType(rexNodes.get(i), type.getFieldList().get(i).getType()))
            .collect(Collectors.toList());
    }

    @Nullable
    public static Object calcValue(
        RexNode rexNode,
        @Nonnull DingoType targetType,
        Object[] tuple,
        DingoType tupleType
    ) throws DingoExprCompileException, FailGetEvaluator {
        Expr expr = convert(rexNode);
        return targetType.convertFrom(
            expr.compileIn(new SqlExprCompileContext(tupleType)).eval(new SqlExprEvalContext(tuple)),
            ExprConverter.INSTANCE
        );
    }

    @Nonnull
    public static Object[] calcValues(
        @Nonnull List<RexNode> rexNodeList,
        @Nonnull DingoType targetType,
        Object[] tuple,
        DingoType tupleType
    ) throws DingoExprCompileException, FailGetEvaluator {
        int size = rexNodeList.size();
        Object[] result = new Object[size];
        for (int i = 0; i < size; ++i) {
            result[i] = calcValue(rexNodeList.get(i), targetType.getChild(i), tuple, tupleType);
        }
        return result;
    }

    private static int typeCodeOf(@Nonnull RelDataType type) {
        return TypeCode.codeOf(type.getSqlTypeName().getName());
    }

    @Nonnull
    private static Op getCastOpWithCheck(RelDataType type, RelDataType inputType) {
        Op op;
        try {
            op = FunFactory.INS.getCastFun(typeCodeOf(type));
            if (op instanceof OpWithEvaluator) { // Check if the evaluator exists.
                EvaluatorFactory factory = ((OpWithEvaluator) op).getFactory();
                factory.getEvaluator(EvaluatorKey.of(typeCodeOf(inputType)));
            }
        } catch (FailGetEvaluator | UndefinedFunctionName e) {
            throw new UnsupportedOperationException(
                "Unsupported cast operation: from \"" + inputType + "\" to \"" + type + "\"."
            );
        }
        return op;
    }

    @Nonnull
    @Override
    public Expr visitInputRef(@Nonnull RexInputRef inputRef) {
        IndexOp op = new IndexOp();
        op.setExprArray(new Expr[]{
            new Var(SqlExprCompileContext.SQL_TUPLE_VAR_NAME),
            Value.of(inputRef.getIndex())
        });
        return op;
    }

    @Nonnull
    @Override
    public Expr visitLiteral(@Nonnull RexLiteral literal) {
        Object value;
        if (literal.getType().getSqlTypeName() == SqlTypeName.SYMBOL) {
            // TODO: should consider the symbol enum, not the string, to avoid misunderstand from a real string.
            value = Objects.requireNonNull(literal.getValue()).toString();
        } else {
            value = convertFromRexLiteral(literal);
        }
        // `null` is implemented by Var in dingo-expr.
        return value != null ? Value.of(value) : Null.INSTANCE;
    }

    @Nonnull
    @Override
    public Expr visitCall(@Nonnull RexCall call) {
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
                op = FunFactory.INS.getFun(kind.name());
                break;
            case CAST:
                RelDataType type = call.getType();
                RexNode operand = call.getOperands().get(0);
                SqlTypeName sqlTypeName = type.getSqlTypeName();
                if (sqlTypeName == SqlTypeName.ARRAY || sqlTypeName == SqlTypeName.MULTISET) {
                    op = FunFactory.INS.getFun(SqlCastListItemsOp.FUN_NAME);
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
                op = FunFactory.INS.getFun("LIST");
                break;
            case MAP_VALUE_CONSTRUCTOR:
                op = FunFactory.INS.getFun("MAP");
                break;
            case OTHER:
                if (call.op.getName().equals("||")) {
                    op = FunFactory.INS.getFun("concat");
                } else {
                    op = FunFactory.INS.getFun(call.op.getName().toLowerCase());
                }
                break;
            case FLOOR:
            case CEIL:
            case OTHER_FUNCTION: {
                op = FunFactory.INS.getFun(call.op.getName().toLowerCase());
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

    @Nonnull
    @Override
    public Expr visitDynamicParam(@Nonnull RexDynamicParam dynamicParam) {
        IndexOp op = new IndexOp();
        op.setExprArray(new Expr[]{
            new Var(SqlExprCompileContext.SQL_DYNAMIC_VAR_NAME),
            Value.of(dynamicParam.getIndex())
        });
        return op;
    }
}
