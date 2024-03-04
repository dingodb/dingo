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
import io.dingodb.exec.operator.params.ReduceParam;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public final class ReduceOperator extends SoleOutOperator {
    public static final ReduceOperator INSTANCE = new ReduceOperator();

    private ReduceOperator() {

    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        ReduceParam param = vertex.getParam();
        param.reduce(tuple);
        return true;
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        synchronized (vertex) {
            ReduceParam param = vertex.getParam();
            Edge edge = vertex.getSoleEdge();
            for (Object[] t : param.getCache()) {
                if (!edge.transformToNext(t)) {
                    break;
                }
            }
            edge.fin(fin);
        }
    }
}
