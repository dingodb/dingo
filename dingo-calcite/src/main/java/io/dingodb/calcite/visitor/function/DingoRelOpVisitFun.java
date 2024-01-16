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

import io.dingodb.calcite.rel.dingo.DingoRelOp;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.RelOpParam;
import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.PipeOp;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.runtime.exception.NeverRunHere;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.function.Supplier;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.exec.utils.OperatorCodeUtils.CACHE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.PIPE_OP;

@Slf4j
public final class DingoRelOpVisitFun {
    private DingoRelOpVisitFun() {
    }

    public static @NonNull Collection<Vertex> visit(
        Job job,
        IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        @NonNull DingoRelOp rel
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);
        Supplier<Vertex> supplier;
        RelOp relOp = rel.getRelOp();
        DingoType schema = DefinitionMapper.mapToDingoType(rel.getInput().getRowType());
        RelOpParam param = new RelOpParam(schema, relOp);
        if (relOp instanceof PipeOp) {
            supplier = () -> new Vertex(PIPE_OP, param);
        } else if (relOp instanceof CacheOp) {
            supplier = () -> new Vertex(CACHE_OP, param);
        } else {
            throw new NeverRunHere();
        }
        return DingoBridge.bridge(idGenerator, inputs, supplier);
    }
}
