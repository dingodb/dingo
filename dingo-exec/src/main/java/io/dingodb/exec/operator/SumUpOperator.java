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
import io.dingodb.exec.operator.params.SumUpParam;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

@Slf4j
public class SumUpOperator extends SoleOutOperator {
    public static final SumUpOperator INSTANCE = new SumUpOperator();

    @Override
    public boolean push(Context context, Object @NonNull [] tuple, Vertex vertex) {
        synchronized (vertex) {
            SumUpParam param = vertex.getParam();
            param.accumulate((long) tuple[0]);
            return true;
        }
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        synchronized (vertex) {
            SumUpParam param = vertex.getParam();
            Edge edge = vertex.getSoleEdge();
            edge.transformToNext(new Object[]{param.getSum()});
            edge.fin(fin);
            // Reset
            param.setSum(0);
        }
    }
}
