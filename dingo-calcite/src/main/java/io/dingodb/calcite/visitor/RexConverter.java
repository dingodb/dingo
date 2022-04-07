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

import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.expr.parser.DingoExprParser;
import io.dingodb.expr.parser.Expr;
import io.dingodb.expr.parser.op.FunFactory;
import io.dingodb.expr.parser.op.IndexOp;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.parser.op.OpFactory;
import io.dingodb.expr.parser.value.Value;
import io.dingodb.expr.parser.var.Var;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public final class RexConverter extends RexVisitorImpl<Expr> {
    private static final RexConverter INSTANCE = new RexConverter();
    private static final List<String> REX_CONST_LITERAL = Stream
        .of("BOTH", "LEADING", "TRAILING")
        .collect(Collectors.toList());

    private RexConverter() {
        super(true);
    }

    public static Expr convert(@Nonnull RexNode rexNode) {
        return rexNode.accept(INSTANCE);
    }

    public static String toString(@Nonnull RexNode rexNode) {
        return rexNode.accept(INSTANCE).toString();
    }

    public static List<String> toString(@Nonnull List<RexNode> rexNodes) {
        return rexNodes.stream()
            .map(RexConverter::toString)
            .collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public Expr visitCall(@Nonnull RexCall call) {
        Op op;
        switch (call.getKind()) {
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
            case AND:
                op = FunFactory.INS.getFun("and");
                break;
            case OR:
                op = FunFactory.INS.getFun("or");
                break;
            case NOT:
                op = OpFactory.getUnary(DingoExprParser.NOT);
                break;
            case CAST:
                switch (call.getType().getSqlTypeName()) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                        op = FunFactory.INS.getFun("int");
                        break;
                    case CHAR:
                    case VARCHAR:
                        op = FunFactory.INS.getFun("string");
                        break;
                    case FLOAT:
                    case DOUBLE:
                        op = FunFactory.INS.getFun("double");
                        break;
                    case DECIMAL:
                        op = FunFactory.INS.getFun("decimal");
                        break;
                    case BOOLEAN:
                        op = FunFactory.INS.getFun("boolean");
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported cast operation: \"" + call + "\".");
                }
                break;
            case TRIM:
                op = FunFactory.INS.getFun("trim");
                break;
            case LTRIM:
                op = FunFactory.INS.getFun("ltrim");
                break;
            case RTRIM:
                op = FunFactory.INS.getFun("rtrim");
                break;
            case OTHER:
                if (call.op.getName() == "||") {
                    op = FunFactory.INS.getFun("concat");
                } else {
                    op = FunFactory.INS.getFun(call.op.getName().toLowerCase());
                }
                break;
            case OTHER_FUNCTION: {
                op = FunFactory.INS.getFun(call.op.getName().toLowerCase());
                break;
            }
            default:
                throw new UnsupportedOperationException("Unsupported operation: \"" + call + "\".");
        }

        List<Expr> exprList = new ArrayList<>();
        for (RexNode node: call.getOperands())  {
            Expr expr = node.accept(this);
            if (REX_CONST_LITERAL.contains(expr.toString())) {
                exprList.add(new Value<String>(expr.toString()));
            } else {
                exprList.add(expr);
            }
        }

        op.setExprArray(exprList.stream().toArray(Expr[]::new));

        /*
        op.setExprArray(call.getOperands().stream()
            .map(o -> o.accept(this))
            .filter(x -> !x.toString().contains("BOTH"))
            .toArray(Expr[]::new));
         */
        return op;
    }

    @Nonnull
    @Override
    public Expr visitLiteral(@Nonnull RexLiteral literal) {
        return Value.of(DingoValues.getValueOf(literal));
    }

    @Nonnull
    @Override
    public Expr visitInputRef(@Nonnull RexInputRef inputRef) {
        IndexOp op = new IndexOp();
        op.setExprArray(new Expr[]{
            new Var("$"),
            Value.of(inputRef.getIndex())
        });
        return op;
    }
}
