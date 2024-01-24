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
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Sink operator has only one input and no output.
 */
public abstract class SinkOperator extends AbstractOperator {
    protected abstract boolean push(Object[] tuple, Vertex vertex);

    @Override
    public synchronized boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        return push(tuple, vertex);
    }

    @Override
    public synchronized void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        fin(fin, vertex);
    }

    protected abstract void fin(Fin fin, Vertex vertex);

}
