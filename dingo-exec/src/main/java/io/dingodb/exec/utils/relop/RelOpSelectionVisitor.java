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

import io.dingodb.expr.rel.RelOpVisitorBase;
import io.dingodb.expr.rel.SourceOp;
import io.dingodb.expr.rel.TandemOp;
import io.dingodb.expr.rel.op.FilterOp;
import io.dingodb.expr.rel.op.GroupedAggregateOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.rel.op.UngroupedAggregateOp;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;


@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RelOpSelectionVisitor extends RelOpVisitorBase<SelectionFlag, @NonNull SelectionObj> {
    public static final RelOpSelectionVisitor INSTANCE = new RelOpSelectionVisitor();

    private static final ExprSelectionVisitor EXPR_SELECTION = ExprSelectionVisitor.INSTANCE;

    @SneakyThrows
    private static @Nullable SelectionFlag visitAggList(@NonNull List<Expr> aggList, @NonNull SelectionObj selected) {
        for (Expr expr : aggList) {
            if (EXPR_SELECTION.visit(expr, selected.getSelection()) != SelectionFlag.OK) {
                return null;
            }
        }
        return SelectionFlag.OK;
    }

    @Override
    public SelectionFlag visitSourceOp(SourceOp op, @NonNull SelectionObj selected) {
        return null;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitFilterOp(@NonNull FilterOp op, @NonNull SelectionObj selected) {
        if (EXPR_SELECTION.visit(op.getFilter(), selected.getSelection()) == SelectionFlag.OK) {
            selected.setProject(false);
            return SelectionFlag.OK;
        }
        return null;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitProjectOp(@NonNull ProjectOp op, @NonNull SelectionObj selected) {
        for (Expr expr : op.getProjects()) {
            if (EXPR_SELECTION.visit(expr, selected.getSelection()) != SelectionFlag.OK) {
                return null;
            }
        }
        selected.setProject(true);
        return SelectionFlag.OK;
    }

    @Override
    public SelectionFlag visitTandemOp(@NonNull TandemOp op, @NonNull SelectionObj selected) {
        if (visit(op.getInput(), selected) == SelectionFlag.OK) {
            return visit(op.getOutput(), selected);
        }
        return null;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitUngroupedAggregateOp(@NonNull UngroupedAggregateOp op, @NonNull SelectionObj selected) {
        return visitAggList(op.getAggList(), selected);
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitGroupedAggregateOp(@NonNull GroupedAggregateOp op, @NonNull SelectionObj selected) {
        EXPR_SELECTION.visit(Exprs.val(op.getGroupIndices()), selected.getSelection());
        return visitAggList(op.getAggList(), selected);
    }
}
