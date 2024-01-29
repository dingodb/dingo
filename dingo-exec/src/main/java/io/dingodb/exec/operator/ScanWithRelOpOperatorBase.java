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
import io.dingodb.common.CoprocessorV2;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.ScanWithRelOpParam;
import io.dingodb.store.api.StoreInstance;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class ScanWithRelOpOperatorBase extends ScanOperatorBase {
    @Override
    protected @NonNull Iterator<Object[]> createIterator(
        @NonNull Context context,
        @NonNull Vertex vertex
    ) {
        ScanWithRelOpParam param = vertex.getParam();
        RangeDistribution rd = context.getDistribution();
        byte[] startKey = rd.getStartKey();
        byte[] endKey = rd.getEndKey();
        boolean includeStart = rd.isWithStart();
        boolean includeEnd = rd.isWithEnd();
        StoreInstance storeInstance = Services.KV_STORE.getInstance(param.getTableId(), rd.getId());
        CoprocessorV2 coprocessor = param.getCoprocessor();
        if (coprocessor == null) {
            return Iterators.transform(
                storeInstance.scan(
                    vertex.getTask().getJobId().seq,
                    new StoreInstance.Range(startKey, endKey, includeStart, includeEnd)
                ),
                wrap(param.getCodec()::decode)::apply
            );
        }
        return Iterators.transform(
            storeInstance.scan(
                vertex.getTask().getJobId().seq,
                new StoreInstance.Range(startKey, endKey, includeStart, includeEnd),
                coprocessor
            ),
            wrap(param.getPushDownCodec()::decode)::apply
        );
    }
}
