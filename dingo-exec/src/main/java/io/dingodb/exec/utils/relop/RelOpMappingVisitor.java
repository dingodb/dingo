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

import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.RelOpVisitorBase;
import io.dingodb.expr.rel.SourceOp;
import io.dingodb.expr.rel.TandemOp;
import io.dingodb.expr.rel.op.FilterOp;
import io.dingodb.expr.rel.op.GroupedAggregateOp;
import io.dingodb.expr.rel.op.ProjectOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.rel.op.UngroupedAggregateOp;
import io.dingodb.expr.runtime.expr.Expr;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;


@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RelOpMappingVisitor extends RelOpVisitorBase<RelOp, List<Integer>> {
    public static final RelOpMappingVisitor INSTANCE = new RelOpMappingVisitor();

    private static final ExprMappingVisitor EXPR_MAPPING_VISITOR = ExprMappingVisitor.INSTANCE;

//    @SneakyThrows
//    private static @Nullable List<Expr> visitAggList(@NonNull List<Expr> aggList, @NonNull RelOp relOp) {
//        for (Expr expr : aggList) {
//            if (EXPR_SELECTION.visit(expr, selected) != SelectionFlag.OK) {
//                return null;
//            }
//        }
//        return SelectionFlag.OK;
//    }

    @Override
    public RelOp visitSourceOp(SourceOp op, @NonNull List<Integer> selection) {
        return null;
    }

    @SneakyThrows
    @Override
    public RelOp visitFilterOp(@NonNull FilterOp op, @NonNull List<Integer> selection) {
        Expr expr1 = EXPR_MAPPING_VISITOR.visit(op.getFilter(), selection);
        RelOp relOp = RelOpBuilder.builder()
            .filter(expr1)
            .build();
        return relOp;
    }

    @SneakyThrows
    @Override
    public RelOp visitProjectOp(@NonNull ProjectOp op, @NonNull List<Integer> selection) {
        Expr[] newProjects = new Expr[op.getProjects().length];
        int i = 0;
        for (Expr expr : op.getProjects()) {
            Expr expr1 = EXPR_MAPPING_VISITOR.visit(expr, selection);
            newProjects[i] = expr1;
            i++;
        }
        RelOp relOp = RelOpBuilder.builder()
            .project(newProjects)
            .build();
        return relOp;
    }

    @Override
    public RelOp visitTandemOp(@NonNull TandemOp op, @NonNull List<Integer> selection) {
        RelOp op1 = visit(op.getInput(), selection);
        RelOp op2 = visit(op.getOutput(), selection);
        RelOp relOp = RelOpBuilder.builder(op1).add(op2).build();
        return relOp;
    }

    @SneakyThrows
    @Override
    public RelOp visitUngroupedAggregateOp(@NonNull UngroupedAggregateOp op, @NonNull List<Integer> selection) {
        return RelOpBuilder.builder().agg(null, op.getAggList()).build();
    }

    @SneakyThrows
    @Override
    public RelOp visitGroupedAggregateOp(@NonNull GroupedAggregateOp op, @NonNull List<Integer> selection) {
        return RelOpBuilder.builder().agg(op.getGroupIndices(), op.getAggList()).build();
    }
}
