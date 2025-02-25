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

import io.dingodb.common.log.LogUtils;
import io.dingodb.expr.common.type.IntType;
import io.dingodb.expr.runtime.expr.BinaryOpExpr;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.ExprVisitorBase;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.expr.runtime.expr.NullaryAggExpr;
import io.dingodb.expr.runtime.expr.NullaryOpExpr;
import io.dingodb.expr.runtime.expr.TertiaryOpExpr;
import io.dingodb.expr.runtime.expr.UnaryAggExpr;
import io.dingodb.expr.runtime.expr.UnaryOpExpr;
import io.dingodb.expr.runtime.expr.Val;
import io.dingodb.expr.runtime.expr.Var;
import io.dingodb.expr.runtime.expr.VariadicOpExpr;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.op.aggregation.CountAllAgg;
import io.dingodb.expr.runtime.op.logical.AndFun;
import io.dingodb.expr.runtime.op.logical.OrFun;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ExprSelectionVisitor extends ExprVisitorBase<SelectionFlag, @NonNull Set<Integer>> {
    public static final ExprSelectionVisitor INSTANCE = new ExprSelectionVisitor();


    private boolean cascadingBinaryLogical(Set<Integer> selected, Expr @NonNull [] operands) throws IOException {
        if (visit(operands[0], selected) != SelectionFlag.OK) {
            return false;
        }
        for (int i = 1; i < operands.length; ++i) {
            if (visit(operands[i], selected) != SelectionFlag.OK) {
                return false;
            }
//            selected.add(operands.)
        }
        return true;
    }

    @Override
    public SelectionFlag visitVal(@NonNull Val expr, Set<Integer> selected) {
        return SelectionFlag.OK;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitVar(@NonNull Var expr, Set<Integer> selected) {
        return SelectionFlag.OK;
    }

    @Override
    public SelectionFlag visitNullaryOpExpr(@NonNull NullaryOpExpr expr, @NonNull Set<Integer> selected) {
        return super.visitNullaryOpExpr(expr, selected);
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitUnaryOpExpr(@NonNull UnaryOpExpr expr, Set<Integer> selected) {
        if (visit(expr.getOperand(), selected) == SelectionFlag.OK) {
            boolean success = false;
            switch (expr.getOpType()) {
                case POS:
                case NEG:
                case NOT:
                case CAST:
                    success = true;
                    break;
                case FUN:
                    switch (expr.getOp().getName()) {
                        default:
                            success = true;
                            break;
                    }
                    break;
                default:
                    break;
            }
            if (success) {
                return SelectionFlag.OK;
            }
        }
        return null;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitBinaryOpExpr(@NonNull BinaryOpExpr expr, Set<Integer> selected) {
        if (visit(expr.getOperand0(), selected) == SelectionFlag.OK && visit(expr.getOperand1(), selected) == SelectionFlag.OK) {
            boolean success = false;
            switch (expr.getOpType()) {
                case ADD:
                case SUB:
                case MUL:
                case DIV:
                case EQ:
                case NE:
                case GT:
                case GE:
                case LT:
                case LE:
                case AND:
                case OR:
                    success = true;
                    break;
                case FUN:
                switch (expr.getOp().getName()) {
                        default:
                            success = true;
                            break;
                    }
                    break;
                default:
                    break;
            }
            if (success) {
                return SelectionFlag.OK;
            }
        }
        return null;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitTertiaryOpExpr(@NonNull TertiaryOpExpr expr, @NonNull Set<Integer> selected) {
        if (visit(expr.getOperand0(), selected) == SelectionFlag.OK
            && visit(expr.getOperand1(), selected) == SelectionFlag.OK
            && visit(expr.getOperand2(), selected) == SelectionFlag.OK
        ) {
            boolean success = false;
            if (expr.getOpType() == OpType.FUN) {
                success = true;
            }
            if (success) {
                return SelectionFlag.OK;
            }
        }
        return null;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitVariadicOpExpr(@NonNull VariadicOpExpr expr, Set<Integer> selected) {
        if (expr.getOpType() == OpType.FUN) {
            boolean success = false;
            switch (expr.getOp().getName()) {
                case AndFun.NAME:
                case OrFun.NAME:
                    success = true;
                    break;
                default:
                    break;
            }
            if (success) {
                return SelectionFlag.OK;
            }
        }
        return null;
    }

    @Override
    public SelectionFlag visitIndexOpExpr(@NonNull IndexOpExpr expr, Set<Integer> selected) {
        if (expr.getOperand1() instanceof Val && expr.getOperand1().getType() instanceof IntType) {
            Integer value = (Integer) ((Val) expr.getOperand1()).getValue();
            selected.add(value);
        }
        return SelectionFlag.OK;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitNullaryAggExpr(@NonNull NullaryAggExpr expr, @NonNull Set<Integer> selected) {
        if (expr.getOp().getName().equals(CountAllAgg.NAME)) {
            return SelectionFlag.OK;
        }
        return null;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitUnaryAggExpr(@NonNull UnaryAggExpr expr, @NonNull Set<Integer> selected) {
        return SelectionFlag.OK;
    }
}
