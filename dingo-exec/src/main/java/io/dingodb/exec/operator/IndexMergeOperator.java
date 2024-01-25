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
import io.dingodb.exec.operator.params.IndexMergeParam;
import io.dingodb.exec.tuple.TupleKey;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.ConcurrentHashMap;

public class IndexMergeOperator extends SoleOutOperator {
    public static final IndexMergeOperator INSTANCE = new IndexMergeOperator();

    private IndexMergeOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        IndexMergeParam params = vertex.getParam();
        params.setContext(context);
        Object[] keyTuple = params.getKeyMapping().revMap(tuple);
        params.getHashMap().put(new TupleKey(keyTuple), params.getSelection().revMap(tuple));
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        IndexMergeParam param = vertex.getParam();
        ConcurrentHashMap<TupleKey, Object[]> hashMap = param.getHashMap();
        hashMap.values().forEach(v -> edge.transformToNext(param.getContext(), v));
        edge.fin(fin);
        // Reset
        param.clear();
    }

}
