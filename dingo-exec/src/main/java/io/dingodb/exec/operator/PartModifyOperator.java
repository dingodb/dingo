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
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PartModifyParam;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class PartModifyOperator extends SoleOutOperator {
    protected PartModifyOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            return pushTuple(context, tuple, vertex);
        }
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        synchronized (vertex) {
            PartModifyParam param = vertex.getParam();
            Edge edge = vertex.getSoleEdge();
            if (!(fin instanceof FinWithException)) {
                edge.transformToNext(new Object[]{param.getCount()});
            }
            edge.fin(fin);
            // Reset
            param.reset();
        }
    }

    protected abstract boolean pushTuple(Context context, @Nullable Object[] tuple, Vertex vertex);
}
