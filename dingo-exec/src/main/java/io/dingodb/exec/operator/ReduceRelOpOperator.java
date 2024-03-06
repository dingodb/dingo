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

package io.dingodb.exec.operator;

import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.ReduceRelOpParam;
import io.dingodb.exec.utils.RelOpUtils;
import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.op.AggregateOp;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public final class ReduceRelOpOperator extends SoleOutOperator {
    public static final ReduceRelOpOperator INSTANCE = new ReduceRelOpOperator();

    private ReduceRelOpOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, @NonNull Vertex vertex) {
        ReduceRelOpParam param = vertex.getParam();
        ((AggregateOp) param.getRelOp()).reduce(tuple);
        return true;
    }

    @Override
    public void fin(int pin, Fin fin, @NonNull Vertex vertex) {
        ReduceRelOpParam param = vertex.getParam();
        Edge edge = vertex.getSoleEdge();
        CacheOp relOp = (CacheOp) param.getRelOp();
        RelOpUtils.forwardCacheOpResults(relOp, vertex.getSoleEdge());
        edge.fin(fin);
        relOp.clear();
    }
}
