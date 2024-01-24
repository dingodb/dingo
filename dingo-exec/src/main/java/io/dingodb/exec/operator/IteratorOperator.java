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

import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;

@Slf4j
public abstract class IteratorOperator extends SoleOutOperator {

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        long count = 0;
        long startTime = System.currentTimeMillis();
        Iterator<Object[]> iterator = createIterator(context, tuple, vertex);
        while (iterator.hasNext()) {
            Object[] newTuple = iterator.next();
            ++count;
            if (!vertex.getSoleEdge().transformToNext(context, newTuple)) {
                break;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("IteratorOperator push, count:{}, cost:{}ms.", count, System.currentTimeMillis() - startTime);
        }
        return false;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        vertex.getSoleEdge().fin(fin);
    }

    protected abstract Iterator<Object[]> createIterator(Context context, Object[] tuple, Vertex vertex);
}
