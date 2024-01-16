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
import io.dingodb.exec.expr.RelOpUtils;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Content;
import io.dingodb.exec.operator.params.RelOpParam;
import io.dingodb.expr.rel.CacheOp;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class CacheOpOperator extends RelOpOperator {
    public static final CacheOpOperator INSTANCE = new CacheOpOperator();

    private CacheOpOperator() {
    }

    @Override
    public void fin(int pin, Fin fin, @NonNull Vertex vertex) {
        final Edge edge = vertex.getSoleEdge();
        CacheOp relOp = (CacheOp) ((RelOpParam) vertex.getParam()).getRelOp();
        RelOpUtils.forwardCacheOpResults(relOp, edge);
        edge.fin(fin);
        relOp.clear();
    }

    @Override
    protected boolean doPush(Content content, @NonNull Vertex vertex, Object[] tuple) {
        CacheOp relOp = (CacheOp) ((RelOpParam) vertex.getParam()).getRelOp();
        relOp.put(tuple);
        return true;
    }
}
