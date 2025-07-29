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
import io.dingodb.exec.operator.params.TableSpoolParam;
import org.checkerframework.checker.nullness.qual.Nullable;

public class TableSpoolOperator extends SoleOutOperator {
    public static TableSpoolOperator INSTANCE = new TableSpoolOperator();

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        TableSpoolParam tableSpoolParam = vertex.getParam();
        tableSpoolParam.tempCollection.add(tuple);
        vertex.getSoleEdge().transformToNext(tuple);
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        TableSpoolParam tableSpoolParam = vertex.getParam();
        tableSpoolParam.list.clear();
        tableSpoolParam.list.addAll(tableSpoolParam.tempCollection);
        tableSpoolParam.tempCollection.clear();
        vertex.getSoleEdge().fin(fin);
    }
}
