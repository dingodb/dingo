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

import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.exception.TaskFinException;
import io.dingodb.exec.fin.ErrorType;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.RootParam;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

@Slf4j
public final class RootOperator extends SinkOperator {
    public static final RootOperator INSTANCE = new RootOperator();
    public static final Object[] FIN = new Object[0];

    private RootOperator() {

    }

    @Override
    public boolean push(Context context, Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            RootParam param = vertex.getParam();
            if (vertex.getTask().getStatus() != Status.RUNNING) {
                return false;
            }
            if (log.isDebugEnabled()) {
                // if table has hide primary key then field count > tuple
                if (param.getSchema().fieldCount() == tuple.length) {
                    log.debug("Put tuple {} into root queue.", param.getSchema().format(tuple));
                }
            }
            param.forcePut(tuple);
            return true;
        }
    }

    @Override
    public void fin(Fin fin, Vertex vertex) {
        RootParam param = vertex.getParam();
        if (fin instanceof FinWithException) {
            param.setErrorFin(fin);
            log.warn("Got FIN with exception: {}", fin.detail());
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Got FIN with detail:\n{}", fin.detail());
            }
            if (fin instanceof FinWithProfiles) {
                List<OperatorProfile> profiles = ((FinWithProfiles) fin).getProfiles();
                if (profiles.size() > 0) {
                    long autoIncId = profiles.get(0).getAutoIncId();
                    if (autoIncId != 0) {
                        param.setAutoIncId(autoIncId);
                    }
                }
            }
        }
        param.forcePut(FIN);
    }

    public Object @NonNull [] popValue(Vertex vertex) {
        RootParam param = vertex.getParam();
        Object[] tuple = param.forceTake();
        TupleMapping selection = param.getSelection();
        if (tuple != FIN && selection != null) {
            Object[] tuple1 = new Object[selection.size()];
            selection.revMap(tuple1, tuple);
            return tuple1;
        }
        return tuple;
    }

    public Long popAutoIncId(Vertex vertex) {
        RootParam param = vertex.getParam();
        return param.getAutoIncId();
    }

    public void checkError(Vertex vertex) {
        RootParam param = vertex.getParam();
        Fin errorFin = param.getErrorFin();
        if (errorFin != null) {
            String errorMsg = errorFin.detail();
            Task task = vertex.getTask();
            if (errorFin instanceof FinWithException) {
                throw new TaskFinException(
                    ((FinWithException) errorFin).getTaskStatus().getErrorType(), errorMsg, task.getJobId());
            } else {
                throw new TaskFinException(ErrorType.Unknown, errorMsg, task.getJobId());
            }
        }
    }
}
