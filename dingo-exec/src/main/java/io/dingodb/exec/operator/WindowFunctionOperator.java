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

import io.dingodb.common.log.LogUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.TaskStatus;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.WindowFunctionParam;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;

@Slf4j
public class WindowFunctionOperator extends SoleOutOperator {
    public static final WindowFunctionOperator INSTANCE = new WindowFunctionOperator();

    private WindowFunctionOperator() {

    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        WindowFunctionParam param = vertex.getParam();
        param.getList().add(tuple);
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        // push next
        WindowFunctionParam param = vertex.getParam();

        try {
            Iterator response = param.getWindowService().transform(param.getList().iterator());
            while (response.hasNext()) {
                Object[] tuple1 = (Object[]) response.next();
                vertex.getSoleEdge().transformToNext(tuple1);
            }
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            TaskStatus taskStatus = new TaskStatus();
            taskStatus.setStatus(false);
            taskStatus.setTaskId(vertex.getTask().getId().toString());
            taskStatus.setErrorMsg(e.getMessage());
            vertex.getSoleEdge().fin(FinWithException.of(taskStatus));
            return;
        }
        // push fin
        vertex.getSoleEdge().fin(fin);
    }
}
