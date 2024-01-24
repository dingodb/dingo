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

package io.dingodb.exec.dag;

import io.dingodb.common.CommonId;
import io.dingodb.exec.OperatorFactory;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
public class Edge {

    private Vertex previous;
    private Vertex next;
    private CommonId partId;

    public Edge(Vertex previous, Vertex next) {
        this.previous = previous;
        this.next = next;
    }

    public boolean transformToNext(Object[] tuple) {
        return transformToNext(Context.builder().build(), tuple);
    }

    public boolean transformToNext(Context context, Object[] tuple) {
        return OperatorFactory.getInstance(next.getOp()).push(context.setPin(previous.getPin()), tuple, next);
    }

    public void fin(Fin fin) {
        OperatorFactory.getInstance(next.getOp()).fin(previous.getPin(), fin, next);
    }

}
