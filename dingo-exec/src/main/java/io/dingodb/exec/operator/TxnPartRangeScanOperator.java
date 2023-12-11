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
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.TxnPartRangeScanParam;
import io.dingodb.store.api.StoreInstance;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

public class TxnPartRangeScanOperator extends FilterProjectSourceOperator {
    public static final TxnPartRangeScanOperator INSTANCE = new TxnPartRangeScanOperator();

    private TxnPartRangeScanOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        TxnPartRangeScanParam param = vertex.getParam();
        byte[] startKey = param.getStartKey();
        byte[] endKey = param.getEndKey();
        boolean includeStart = param.isIncludeStart();
        boolean includeEnd = param.isIncludeEnd();
        Coprocessor coprocessor = param.getCoprocessor();
        Iterator<Object[]> iterator;
        StoreInstance storeInstance = Services.LOCAL_STORE.getInstance(param.getTableId(), param.getPartId());
        // TODO Set flag in front of the byte key
        if (coprocessor == null) {
            iterator = Iterators.transform(
                storeInstance.scan(new StoreInstance.Range(startKey, endKey, includeStart, includeEnd)),
                wrap(param.getCodec()::decode)::apply);
        } else {
            iterator = Iterators.transform(
                storeInstance.scan(new StoreInstance.Range(startKey, endKey, includeStart, includeEnd), coprocessor),
                wrap(param.getCodec()::decode)::apply);
        }
        return iterator;
    }
}
