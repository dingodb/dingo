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
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PartRangeDeleteParam;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public final class PartRangeDeleteOperator extends SoleOutOperator {
    public static final PartRangeDeleteOperator INSTANCE = new PartRangeDeleteOperator();

    private PartRangeDeleteOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        RangeDistribution distribution = context.getDistribution();
        PartRangeDeleteParam param = vertex.getParam();
        StoreInstance store = Services.KV_STORE.getInstance(param.getTableId(), distribution.getId());
        final long startTime = System.currentTimeMillis();
        long count = store.delete(new StoreInstance.Range(distribution.getStartKey(), distribution.getEndKey(),
            distribution.isWithStart(), distribution.isWithEnd()));
        vertex.getSoleEdge().transformToNext(context, new Object[]{count});
        if (log.isDebugEnabled()) {
            log.debug("Delete data by range, delete count: {}, cost: {} ms.",
                count, System.currentTimeMillis() - startTime);
        }
        return false;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        vertex.getSoleEdge().fin(fin);
    }
}
