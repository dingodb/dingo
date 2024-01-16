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
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Content;
import io.dingodb.exec.operator.params.ScanWithNoOpParam;
import io.dingodb.store.api.StoreInstance;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class ScanWithRelOpOperator extends SoleOutOperator {
    protected static @NonNull Iterator<Object[]> createIterator(
        @NonNull Content content,
        @NonNull Vertex vertex
    ) {
        ScanWithNoOpParam param = vertex.getParam();
        RangeDistribution rd = content.getDistribution();
        byte[] startKey = rd.getStartKey();
        byte[] endKey = rd.getEndKey();
        boolean includeStart = rd.isWithStart();
        boolean includeEnd = rd.isWithEnd();
        StoreInstance storeInstance = Services.KV_STORE.getInstance(param.getTableId(), rd.getId());
        return Iterators.transform(
            storeInstance.scan(new StoreInstance.Range(startKey, endKey, includeStart, includeEnd)),
            wrap(param.getCodec()::decode)::apply
        );
    }

    @Override
    public boolean push(Content content, @Nullable Object[] tuple, Vertex vertex) {
        long count = 0;
        long startTime = System.currentTimeMillis();
        Iterator<Object[]> iterator = createIterator(content, vertex);
        count = doPush(content, vertex, iterator);
        if (log.isDebugEnabled()) {
            log.debug("count: {}, cost: {}ms.", count, System.currentTimeMillis() - startTime);
        }
        return false;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        vertex.getSoleEdge().fin(fin);
    }

    protected abstract long doPush(Content content, @NonNull Vertex vertex, @NonNull Iterator<Object[]> sourceIterator);
}
