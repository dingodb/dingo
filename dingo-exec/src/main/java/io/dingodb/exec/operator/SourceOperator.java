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
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.SourceParam;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Source operator has no inputs and only one output.
 */
@Slf4j
public abstract class SourceOperator extends SoleOutOperator {

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        return push(context, vertex);
    }

    public abstract boolean push(Context context, Vertex vertex);

    @Override
    public  void fin(int pin, Fin fin, Vertex vertex) {
        SourceParam param = (SourceParam) vertex.getData();
        if (fin instanceof FinWithException) {
            vertex.getOutList().forEach(e -> e.fin(fin));
        } else {
            vertex.getOutList().forEach(e -> e.fin(new FinWithProfiles(param.getProfiles())));
        }
        param.clear();
    }

}
