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

import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.dingo.DingoScanWithRelOp;
import io.dingodb.calcite.rel.dingo.DingoStreamingConverter;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.AggregateParams;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.exec.utils.OperatorCodeUtils.AGGREGATE;

public class DingoAggregateVisitFun {

    @NonNull
    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        DingoJobVisitor visitor, DingoAggregate rel
    ) {
        RelNode input = rel.getInput();
        Collection<Vertex> inputs = dingo(input).accept(visitor);
        return DingoBridge.bridge(idGenerator, inputs, new OperatorSupplier(rel, input));
    }

    @AllArgsConstructor
    static class OperatorSupplier implements Supplier<Vertex> {

        final DingoAggregate rel;
        final RelNode input;

        @Override
        public Vertex get() {
            DingoType inputType = DefinitionMapper.mapToDingoType(input.getRowType());

            // Extract the project expression list of the input RelNode, which
            // is used to parse the separator literal of LISTAGG GROUP_CONCAT
            List<RexNode> projects = extractProjects(input);

            List<Agg> aggList = AggFactory.getAggList(
                rel.getAggCallList(),
                inputType,
                projects
            );

            AggregateParams params = new AggregateParams(
                AggFactory.getAggKeys(rel.getGroupSet()),
                aggList
            );
            return new Vertex(AGGREGATE, params);
        }

        @Nullable
        private static List<RexNode> extractProjects(RelNode relNode) {
            if (relNode instanceof LogicalProject) {
                return ((LogicalProject) relNode).getProjects();
            }
            if (relNode instanceof DingoStreamingConverter) {
                RelNode input = ((DingoStreamingConverter) relNode).getInput();
                if (input instanceof DingoProject) {
                    return ((DingoProject) input).getProjects();
                }
                if (input instanceof DingoScanWithRelOp) {
                    ((DingoScanWithRelOp) input).getRelOp();
                }
            }
            return null;
        }
    }
}
