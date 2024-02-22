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

import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PartCountParam;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class PartCountOperator extends SourceOperator {
    public static final PartCountOperator INSTANCE = new PartCountOperator();

    private PartCountOperator() {
    }

    @Override
    public boolean push(Context context, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        PartCountParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile(vertex.getId());
        profile.setStartTimeStamp(System.currentTimeMillis());
        final long startTime = System.currentTimeMillis();
        long count = 0; // todo count must have range, delete this class?;
        edge.transformToNext(new Object[]{count});
        if (log.isDebugEnabled()) {
            log.debug("Count table by partition, get count: {}, cost: {} ms.",
                count, System.currentTimeMillis() - startTime);
        }
        profile.setProcessedTupleCount(count);
        profile.setEndTimeStamp(System.currentTimeMillis());
        return false;
    }
}
