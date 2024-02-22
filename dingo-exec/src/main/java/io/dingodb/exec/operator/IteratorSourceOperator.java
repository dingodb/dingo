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
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.SourceParam;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

@Slf4j
public abstract class IteratorSourceOperator extends SourceOperator {
    @Override
    public boolean push(Context context, Vertex vertex) {
        long count = 0;
        long startTime = System.currentTimeMillis();
        SourceParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile(vertex.getId());
        profile.setStartTimeStamp(startTime);
        Iterator<Object[]> iterator = createIterator(vertex);
        while (iterator.hasNext()) {
            Object[] tuple = iterator.next();
            if (tuple[0] instanceof RangeDistribution) {
                context.setDistribution((RangeDistribution) tuple[0]);
                if (tuple.length > 1) {
                    tuple = (Object[]) tuple[1];
                }
            }
            ++count;
            for (Edge edge : vertex.getOutList()) {
                if (!edge.transformToNext(context, tuple)) {
                    break;
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("IteratorSourceOperator push,  count: {}, cost: {}ms.", count,
                System.currentTimeMillis() - startTime);
        }
        profile.setProcessedTupleCount(count);
        profile.setEndTimeStamp(System.currentTimeMillis());
        return false;
    }

    protected abstract @NonNull Iterator<Object[]> createIterator(Vertex vertex);
}
