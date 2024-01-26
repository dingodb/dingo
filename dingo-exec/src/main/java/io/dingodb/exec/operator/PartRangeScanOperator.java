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

import com.google.common.collect.Iterators;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PartRangeScanParam;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public final class PartRangeScanOperator extends FilterProjectOperator {
    public static final PartRangeScanOperator INSTANCE = new PartRangeScanOperator();

    private PartRangeScanOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Context context, Object[] tuple, Vertex vertex) {
        RangeDistribution distribution = context.getDistribution();
        PartRangeScanParam param = vertex.getParam();
        byte[] startKey = distribution.getStartKey();
        byte[] endKey = distribution.getEndKey();
        boolean includeStart = distribution.isWithStart();
        boolean includeEnd = distribution.isWithEnd();
        Coprocessor coprocessor = param.getCoprocessor();
        Iterator<Object[]> iterator;
        StoreInstance storeInstance = Services.KV_STORE.getInstance(param.getTableId(), distribution.getId());
        if (coprocessor == null) {
            iterator = Iterators.transform(
                storeInstance.scan(vertex.getTask().getJobId().seq, new StoreInstance.Range(startKey, endKey, includeStart, includeEnd)),
                wrap(param.getCodec()::decode)::apply);
        } else {
            iterator = Iterators.transform(
                storeInstance.scan(vertex.getTask().getJobId().seq, new StoreInstance.Range(startKey, endKey, includeStart, includeEnd), coprocessor),
                wrap(param.getCodec()::decode)::apply);
        }
        return iterator;
    }

}
