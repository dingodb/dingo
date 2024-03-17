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
import io.dingodb.exec.operator.params.RelOpParam;
import io.dingodb.exec.utils.RelOpUtils;
import io.dingodb.expr.rel.CacheOp;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class CacheOpOperator extends SoleOutOperator {
    public static final CacheOpOperator INSTANCE = new CacheOpOperator();

    private CacheOpOperator() {
    }

    @Override
    public void fin(int pin, Fin fin, @NonNull Vertex vertex) {
        final Edge edge = vertex.getSoleEdge();
        CacheOp relOp = (CacheOp) ((RelOpParam) vertex.getParam()).getRelOp();
        synchronized (relOp) {
            RelOpUtils.forwardCacheOpResults(relOp, edge);
            edge.fin(fin);
            relOp.clear();
        }
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, @NonNull Vertex vertex) {
        CacheOp relOp = (CacheOp) ((RelOpParam) vertex.getParam()).getRelOp();
        synchronized (relOp) {
            relOp.put(tuple);
        }
        return true;
    }
}
