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
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PartDeleteParam;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class PartDeleteOperator extends PartModifyOperator {
    public static final PartDeleteOperator INSTANCE = new PartDeleteOperator();

    private PartDeleteOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        PartDeleteParam param = vertex.getParam();
        RangeDistribution distribution = context.getDistribution();
        StoreInstance store = Services.KV_STORE.getInstance(param.getTableId(), distribution.getId());
        if (store.deleteWithIndex(tuple)) {
            if (store.deleteIndex(tuple)) {
                param.inc();
                context.addKeyState(true);
            } else {
                context.addKeyState(false);
            }
        } else {
            context.addKeyState(false);
        }
        return true;
    }
}
