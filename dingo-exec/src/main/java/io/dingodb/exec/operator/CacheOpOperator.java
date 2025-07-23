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
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.profile.Profile;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.fin.TaskStatus;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.RelOpParam;
import io.dingodb.exec.utils.RelOpUtils;
import io.dingodb.expr.rel.CacheOp;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public final class CacheOpOperator extends SoleOutOperator {
    public static final CacheOpOperator INSTANCE = new CacheOpOperator();

    private CacheOpOperator() {
    }

    @Override
    public void fin(int pin, Fin fin, @NonNull Vertex vertex) {
        final Edge edge = vertex.getSoleEdge();
        RelOpParam param = vertex.getParam();
        Profile profile = param.getProfile();
        if (profile == null) {
            profile = param.getProfile("aggCache");
        }
        CacheOp relOp = (CacheOp) (param).getRelOp();
        long start = System.currentTimeMillis();
        synchronized (relOp) {
            try {
                RelOpUtils.forwardCacheOpResults(relOp, edge);
                if (profile instanceof OperatorProfile) {
                    OperatorProfile operatorProfile = (OperatorProfile) profile;
                    operatorProfile.pipeOpTime(start);
                }
            } catch (Exception e) {
                LogUtils.error(log, "[task-fin] fin exception:{}", e.getMessage(), e);
                TaskStatus taskStatus = new TaskStatus();
                taskStatus.setStatus(false);
                taskStatus.setTaskId(vertex.getTask().getId().toString());
                taskStatus.setErrorMsg(e.getMessage());
                edge.fin(FinWithException.of(taskStatus));
                return;
            }
            profile.end();
            if (fin instanceof FinWithProfiles) {
                FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                finWithProfiles.addProfile(profile);
            }
            edge.fin(fin);
            relOp.clear();
        }
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, @NonNull Vertex vertex) {
        RelOpParam param = vertex.getParam();
        OperatorProfile operatorProfile = param.getProfile("aggCache");
        CacheOp relOp = (CacheOp) (param).getRelOp();
        long start = System.currentTimeMillis();
        synchronized (relOp) {
            relOp.put(tuple);
        }
        operatorProfile.time(start);
        return true;
    }
}
