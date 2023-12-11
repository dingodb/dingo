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

import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.operator.params.PartRangeDeleteParam;
import io.dingodb.exec.operator.params.TxnPartRangeDeleteParam;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class TxnPartRangeDeleteOperator extends SourceOperator {
    public static final TxnPartRangeDeleteOperator INSTANCE = new TxnPartRangeDeleteOperator();

    private TxnPartRangeDeleteOperator() {
    }

    @Override
    public boolean push(Vertex vertex) {
        TxnPartRangeDeleteParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile(vertex.getId());
        profile.setStartTimeStamp(System.currentTimeMillis());
        StoreInstance store = Services.LOCAL_STORE.getInstance(param.getTableId(), param.getPartId());
        final long startTime = System.currentTimeMillis();
        // TODO Set flag in front of the byte key
        long count = store.delete(new StoreInstance.Range(
            param.getStartKey(), param.getEndKey(), param.isIncludeStart(), param.isIncludeEnd()));
        vertex.getSoleEdge().transformToNext(new Object[]{count});
        if (log.isDebugEnabled()) {
            log.debug("Delete data by range, delete count: {}, cost: {} ms.",
                count, System.currentTimeMillis() - startTime);
        }
        profile.setProcessedTupleCount(count);
        profile.setEndTimeStamp(System.currentTimeMillis());
        return false;
    }
}
