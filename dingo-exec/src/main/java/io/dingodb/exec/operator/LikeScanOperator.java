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
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.LikeScanParam;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public final class LikeScanOperator extends FilterProjectSourceOperator {
    public static final LikeScanOperator INSTANCE = new LikeScanOperator();

    private LikeScanOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        LikeScanParam param = vertex.getParam();
        StoreInstance store = Services.KV_STORE.getInstance(param.getTableId(), param.getPartId());
        return Iterators.transform(
            store.scan(new StoreInstance.Range(param.getPrefix(), param.getPrefix(), true, true)),
            wrap(param.getCodec()::decode)::apply);
    }

}
