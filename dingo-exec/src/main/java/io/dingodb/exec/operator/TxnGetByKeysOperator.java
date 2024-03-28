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

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.GetByKeysParam;
import io.dingodb.exec.operator.params.TxnGetByKeysParam;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@Slf4j
public final class TxnGetByKeysOperator extends FilterProjectOperator {
    public static final TxnGetByKeysOperator INSTANCE = new TxnGetByKeysOperator();

    private TxnGetByKeysOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Context context, Object[] tuple, Vertex vertex) {
        TxnGetByKeysParam param = vertex.getParam();
        param.setContext(context);
        byte[] keys = param.getCodec().encodeKey(tuple);
        CommonId tableId = param.getTableId();
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId partId = context.getDistribution().getId();
        CodecService.getDefault().setId(keys, partId.domain);
        byte[] partIdByte = partId.encode();
        Iterator<Object[]> local = getLocalStore(
            partId,
            param.getCodec(),
            keys,
            tableId,
            txnId,
            partIdByte,
            vertex.getTask().getTransactionType());
        if (local != null) return local;
        StoreInstance store;
        store = Services.KV_STORE.getInstance(tableId, partId);
        KeyValue keyValue = store.txnGet(param.getScanTs(), keys, param.getTimeOut());
        if (keyValue == null || keyValue.getValue() == null) {
            if (vertex.getTask().getTransactionType() == TransactionType.PESSIMISTIC && !param.isSelect()) {
                return Collections.singletonList(tuple).iterator();
            } else {
                return Collections.emptyIterator();
            }
        }
        Object[] result = param.getCodec().decode(keyValue);
        return Collections.singletonList(result).iterator();
    }

    @Nullable
    public static Iterator<Object[]> getLocalStore(CommonId partId,
                                                   KeyValueCodec codec,
                                                   byte[] keys,
                                                   CommonId tableId, CommonId txnId,
                                                   byte[] partIdByte,
                                                   TransactionType transactionType) {
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
        byte[] dataKey = ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            keys,
            Op.PUTIFABSENT.getCode(),
            len,
            txnIdByte, tableIdByte, partIdByte);
        byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
        deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
        byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
        updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
        List<byte[]> bytes = new ArrayList<>(3);
        bytes.add(dataKey);
        bytes.add(deleteKey);
        bytes.add(updateKey);
        StoreInstance store;
        store = Services.LOCAL_STORE.getInstance(tableId, partId);
        List<KeyValue> keyValues = store.get(bytes);
        if (keyValues != null && keyValues.size() > 0) {
            if (keyValues.size() > 1) {
                throw new RuntimeException(txnId + " Key is not existed than two in local store");
            }
            KeyValue value = keyValues.get(0);
            byte[] oldKey = value.getKey();
            if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                KeyValue keyValue = new KeyValue(keys, value.getValue());
                Object[] result = codec.decode(keyValue);
                return Collections.singletonList(result).iterator();
            } else {
                if (transactionType == TransactionType.PESSIMISTIC) {
                    KeyValue keyValue = store.get(ByteUtils.getKeyByOp(
                        CommonId.CommonType.TXN_CACHE_LOCK,
                        Op.LOCK,
                        dataKey)
                    );
                    // first primary key
                    if (keyValue == null) {
                        return null;
                    }
                }
                return Collections.emptyIterator();
            }
        }
        return null;
    }
}
