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

import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.GetByKeysParam;
import io.dingodb.exec.operator.params.TxnGetByKeysParam;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.Iterator;

@Slf4j
public final class TxnGetByKeysOperator extends FilterProjectOperator {
    public static final TxnGetByKeysOperator INSTANCE = new TxnGetByKeysOperator();

    private TxnGetByKeysOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Context context, Object[] tuple, Vertex vertex) {
        TxnGetByKeysParam param = vertex.getParam();
        byte[] keys = param.getCodec().encodeKey(tuple);
        StoreInstance store;
        store = Services.LOCAL_STORE.getInstance(context.getIndexId(), context.getDistribution().getId());
        KeyValue localKeyValue = store.get(keys);
        if (localKeyValue != null) {
            byte[] key = localKeyValue.getKey();
            if (key[key.length - 2] == Op.PUT.getCode() || key[key.length - 2] == Op.PUTIFABSENT.getCode()) {
                Object[] decode = ByteUtils.decode(localKeyValue);
                KeyValue keyValue = new KeyValue((byte[]) decode[5], localKeyValue.getValue());
                Object[] result = param.getCodec().decode(keyValue);
                return Collections.singletonList(result).iterator();
            }
        }
        store = Services.KV_STORE.getInstance(param.getTableId(), context.getDistribution().getId());
        KeyValue keyValue = store.txnGet(param.getScanTs(), param.getCodec().encodeKey(tuple), param.getTimeOut());
        if (keyValue == null || keyValue.getValue() == null) {
            return Collections.emptyIterator();
        }
        Object[] result = param.getCodec().decode(keyValue);
        return Collections.singletonList(result).iterator();
    }
}
