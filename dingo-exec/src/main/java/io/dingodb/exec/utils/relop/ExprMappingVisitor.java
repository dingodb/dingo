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

package io.dingodb.exec.utils.relop;

import io.dingodb.expr.common.type.IntType;
import io.dingodb.expr.runtime.expr.BinaryOpExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.ExprVisitorBase;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.expr.runtime.expr.NullaryAggExpr;
import io.dingodb.expr.runtime.expr.NullaryOpExpr;
import io.dingodb.expr.runtime.expr.TertiaryOpExpr;
import io.dingodb.expr.runtime.expr.UnaryAggExpr;
import io.dingodb.expr.runtime.expr.UnaryOpExpr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.expr.Var;
import io.dingodb.expr.runtime.expr.VariadicOpExpr;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

import static io.dingodb.exec.utils.relop.SelectionMapping.findIndex;


@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ExprMappingVisitor extends ExprVisitorBase<Expr, @NonNull List<Integer>> {
    public static final ExprMappingVisitor INSTANCE = new ExprMappingVisitor();


//    private boolean cascadingBinaryLogical(Set<Integer> selected, Expr @NonNull [] operands) throws IOException {
//        if (visit(operands[0], selected) != SelectionFlag.OK) {
//            return false;
//        }
//        for (int i = 1; i < operands.length; ++i) {
//            if (visit(operands[i], selected) != SelectionFlag.OK) {
//                return false;
//            }
////            selected.add(operands.)
//        }
//        return true;
//    }

    @Override
    public Expr visitVal(@NonNull Val expr, @NonNull List<Integer> selection) {
        return new ValMappingVisitor(expr).visit(expr.getType(), selection);
    }

    @SneakyThrows
    @Override
    public Expr visitVar(@NonNull Var expr, List<Integer> selection) {
        return Exprs.var(expr.getId());
    }

    @Override
    public Expr visitNullaryOpExpr(@NonNull NullaryOpExpr expr, @NonNull List<Integer> selection) {
        return Exprs.op(expr.getOp());
    }

    @SneakyThrows
    @Override
    public Expr visitUnaryOpExpr(@NonNull UnaryOpExpr expr, @NonNull List<Integer> selection) {
        Expr expr1 = visit(expr.getOperand(), selection);
        return Exprs.op(expr.getOp(), expr1);
    }

    @SneakyThrows
    @Override
    public Expr visitBinaryOpExpr(@NonNull BinaryOpExpr expr, @NonNull List<Integer> selection) {
        Expr expr0 = visit(expr.getOperand0(), selection);
        Expr expr1 = visit(expr.getOperand1(), selection);
        return Exprs.op(expr.getOp(), expr0, expr1);
    }

    @SneakyThrows
    @Override
    public Expr visitTertiaryOpExpr(@NonNull TertiaryOpExpr expr, @NonNull List<Integer> selection) {
        Expr expr0 = visit(expr.getOperand0(), selection);
        Expr expr1 = visit(expr.getOperand1(), selection);
        Expr expr2 = visit(expr.getOperand2(), selection);
        return Exprs.op(expr.getOp(), expr0, expr1, expr2);
    }

    @SneakyThrows
    @Override
    public Expr visitVariadicOpExpr(@NonNull VariadicOpExpr expr, @NonNull List<Integer> selection) {

        Expr[] operands = expr.getOperands();
        Object[] newOperands = new Expr[operands.length];
        for (int i = 0; i < operands.length; i++) {
            Expr expr1 = visit(operands[i], selection);
            newOperands[i] = expr1;
        }
        return Exprs.op(expr.getOp(), newOperands);
    }

    @Override
    public Expr visitIndexOpExpr(@NonNull IndexOpExpr expr, @NonNull List<Integer> selection) {
        Expr expr0 = visit(expr.getOperand0(), selection);
        if (expr.getOperand1() instanceof Val && expr.getOperand1().getType() instanceof IntType) {
            Integer value = (Integer) ((Val) expr.getOperand1()).getValue();
            if (value != null) {
                value = findIndex(selection, value);
            }
            return Exprs.op(Exprs.INDEX, expr0, Exprs.val(value, expr.getOperand1().getType()));
        }
        Expr expr1 = visit(expr.getOperand1(), selection);
        return Exprs.op(Exprs.INDEX, expr0, expr1);
    }

    @SneakyThrows
    @Override
    public Expr visitNullaryAggExpr(@NonNull NullaryAggExpr expr, @NonNull List<Integer> selection) {
        return Exprs.op(expr.getOp());
    }

    @SneakyThrows
    @Override
    public Expr visitUnaryAggExpr(@NonNull UnaryAggExpr expr, @NonNull List<Integer> selection) {
        Expr operand = visit(expr.getOperand(), selection);
        return Exprs.op(expr.getOp(), operand);
    }
}
