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
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public abstract class FanOutOperator extends AbstractOperator {

    protected abstract int calcOutputIndex(Context context, Object @NonNull [] tuple, Vertex vertex);

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        /*int index = calcOutputIndex(content, tuple, vertex);
        if (log.isDebugEnabled()) {
            log.debug("Tuple is pushing to output {}.", index);
        }
        return vertex.getOutList().get(index).transformToNext(content, tuple);*/
        return vertex.getSoleEdge().transformToNext(context, tuple);
    }

    @Override
    public  void fin(int pin, Fin fin, Vertex vertex) {
        if (log.isDebugEnabled()) {
            log.debug("Got FIN, push it to {} outputs", vertex.getOutList().size());
        }
        for (Edge edge : vertex.getOutList()) {
            edge.fin(fin);
        }
    }

}
