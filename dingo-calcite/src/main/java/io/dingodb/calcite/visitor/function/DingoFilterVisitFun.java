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

package io.dingodb.calcite.visitor.function;

import io.dingodb.calcite.rel.DingoFilter;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.FilterParam;
import lombok.AllArgsConstructor;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.function.Supplier;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.exec.utils.OperatorCodeUtils.FILTER;

public class DingoFilterVisitFun {
    @NonNull
    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, DingoFilter rel
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);
        RexNode condition = rel.getCondition();
        return DingoBridge.bridge(idGenerator, inputs, new OperatorSupplier(condition, rel));
    }

    @AllArgsConstructor
    static class OperatorSupplier implements Supplier<Vertex> {

        final RexNode condition;
        final DingoFilter rel;

        @Override
        public Vertex get() {
            FilterParam params = new FilterParam(
                SqlExprUtils.toSqlExpr(condition), DefinitionMapper.mapToDingoType(rel.getInput().getRowType()));

            return new Vertex(FILTER, params);
        }
    }

}
