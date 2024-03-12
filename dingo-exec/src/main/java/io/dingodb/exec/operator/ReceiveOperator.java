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

import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.ReceiveParam;
import io.dingodb.exec.tuple.TupleId;
import io.dingodb.exec.utils.QueueUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ReceiveOperator extends SourceOperator {
    public static final ReceiveOperator INSTANCE = new ReceiveOperator();

    private ReceiveOperator() {

    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        /*
          when the upstream operator('sender') has failed,
          then the current operator('receiver') should fail too
          so the `Fin` should use FinWithException
         */
        ReceiveParam param = vertex.getParam();
        Fin finObj = param.getFinObj();
        if (finObj instanceof FinWithException) {
            super.fin(pin, finObj, vertex);
        } else {
            super.fin(pin, fin, vertex);
        }
    }

    @Override
    public boolean push(Context context, Vertex vertex) {
        ReceiveParam param = vertex.getParam();

        long count = 0;
        OperatorProfile profile = param.getProfile(vertex.getId());
        profile.setStartTimeStamp(System.currentTimeMillis());
        while (true) {
            TupleId tupleId = QueueUtils.forceTake(param.getTupleQueue());
            Object[] tuple = tupleId.getTuple();
            if (!(tuple[0] instanceof Fin)) {
                RangeDistribution distribution = null;
                if (tupleId.getPartId() != null) {
                    distribution = RangeDistribution.builder().id(tupleId.getPartId()).build();
                }
                if (tupleId.getIndexId() != null) {
                    context.setIndexId(tupleId.getIndexId());
                }
                ++count;
                if (log.isDebugEnabled()) {
                    log.debug("(tag = {}) Take out tuple {} from receiving queue.",
                        param.getTag(),
                        param.getSchema().format(tuple)
                    );
                }
                context.setDistribution(distribution);
                if (!vertex.getSoleEdge().transformToNext(context, tuple)) {
                    param.getEndpoint().stop();
                    // Stay in loop to receive FIN.
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("(tag = {}) Take out FIN.", param.getTag());
                }
                profile.setEndTimeStamp(System.currentTimeMillis());
                profile.setProcessedTupleCount(count);
                Fin fin = (Fin) tuple[0];
                if (fin instanceof FinWithProfiles) {
                    param.addAll(((FinWithProfiles) fin).getProfiles());
                } else if (fin instanceof FinWithException) {
                    param.setFinObj(fin);
                }
                break;
            }
        }
        return false;
    }
}
