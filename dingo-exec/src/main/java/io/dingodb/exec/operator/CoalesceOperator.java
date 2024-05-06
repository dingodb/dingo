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

import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.log.LogUtils;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.CoalesceParam;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public final class CoalesceOperator extends SoleOutOperator {

    public static final CoalesceOperator INSTANCE = new CoalesceOperator();

    public CoalesceOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            LogUtils.debug(log, "Got tuple from pin {}.", context.getPin());
            return vertex.getSoleEdge().transformToNext(context, tuple);
        }
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        synchronized (vertex) {
            CoalesceParam param = vertex.getParam();
            OperatorProfile profile = param.getProfile("coalesce");
            long start = System.currentTimeMillis();
            LogUtils.debug(log, "Got FIN from pin {}.", pin);
            Edge edge = vertex.getSoleEdge();
            if (fin instanceof FinWithException) {
                edge.fin(fin);
                return;
            }
            setFin(pin, fin, param);
            profile.time(start);
            if (isAllFin(param)) {
                if (fin instanceof FinWithProfiles) {
                    FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                    finWithProfiles.addProfile(vertex);
                }
                edge.fin(fin);
                param.clear();
            } else {
                if (fin instanceof FinWithProfiles) {
                    FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                    profile.getChildren().add(finWithProfiles.getProfile());
                }
            }
        }
    }

    private void setFin(int pin, Fin fin, CoalesceParam param) {
        int inputNum = param.getInputNum();
        assert pin < inputNum : "Pin no is greater than the max (" + inputNum + ").";
        assert !param.getFinFlags()[pin] : "Input on pin (" + pin + ") is already finished.";
        param.setFinFlags(pin);
    }

    private boolean isAllFin(CoalesceParam param) {
        for (boolean b : param.getFinFlags()) {
            if (!b) {
                return false;
            }
        }
        return true;
    }

}
