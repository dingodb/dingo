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

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartRangeScanParam;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnPartRangeScanOperator extends FilterProjectOperator {
    public static final TxnPartRangeScanOperator INSTANCE = new TxnPartRangeScanOperator();

    private TxnPartRangeScanOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Context context, Object[] tuple, Vertex vertex) {
        RangeDistribution distribution = context.getDistribution();
        TxnPartRangeScanParam param = vertex.getParam();
        byte[] startKey = distribution.getStartKey();
        byte[] endKey = distribution.getEndKey();
        boolean includeStart = distribution.isWithStart();
        boolean includeEnd = distribution.isWithEnd();
        Coprocessor coprocessor = param.getCoprocessor();
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId tableId = param.getTableId();
        CommonId partId = distribution.getId();
        CodecService.getDefault().setId(startKey, partId.domain);
        CodecService.getDefault().setId(endKey, partId.domain);
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);

        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        byte[] partIdByte = partId.encode();
        byte[] encodeStart = ByteUtils.encode(CommonId.CommonType.TXN_CACHE_DATA, startKey, Op.NONE.getCode(),
            (txnIdByte.length + tableIdByte.length + partIdByte.length), txnIdByte, tableIdByte, partIdByte);
        byte[] encodeEnd = ByteUtils.encode(CommonId.CommonType.TXN_CACHE_DATA, endKey, Op.NONE.getCode(),
            (txnIdByte.length + tableIdByte.length + partIdByte.length), txnIdByte, tableIdByte, partIdByte);
        Iterator<KeyValue> localKVIterator;
        Iterator<KeyValue> kvKVIterator;
        if (coprocessor == null) {
            localKVIterator = Iterators.transform(
                localStore.scan(new StoreInstance.Range(encodeStart, encodeEnd, includeStart, includeEnd)),
                wrap(ByteUtils::mapping)::apply);
            kvKVIterator = kvStore.txnScan(param.getScanTs(), new StoreInstance.Range(startKey, endKey, includeStart, includeEnd), param.getTimeOut());
        } else {
            localKVIterator = Iterators.transform(
                localStore.scan(new StoreInstance.Range(encodeStart, encodeEnd, includeStart, includeEnd), coprocessor),
                wrap(ByteUtils::mapping)::apply);
            kvKVIterator = kvStore.txnScan(param.getScanTs(), new StoreInstance.Range(startKey, endKey, includeStart, includeEnd), param.getTimeOut());
        }

        KeyValue kv1 = getNextValue(localKVIterator);
        KeyValue kv2 = getNextValue(kvKVIterator);

        final int pos = 9;
        List<KeyValue> mergedList = new ArrayList<>();
        List<KeyBytes> deletedList = new ArrayList<>();

        while (kv1 != null && kv2 != null) {
            byte[] key1 = kv1.getKey();
            byte[] key2 = kv2.getKey();
            int code = key1[key1.length - 2];
            if (ByteArrayUtils.lessThan(key1, key2, pos, key1.length - 2)) {
                if ((code == Op.PUT.getCode() || code == Op.PUTIFABSENT.getCode()) && !deletedList.contains(new KeyBytes(key2))) {
                    mergedList.add(kv1);
                    kv1 = getNextValue(localKVIterator);
                    continue;
                } else if (code == Op.DELETE.getCode()) {
                    deletedList.add(new KeyBytes(key1));
                    kv1 = getNextValue(localKVIterator);
                    continue;
                }
            }
            if (ByteArrayUtils.greatThan(key1, key2, pos, key1.length - 2)) {
                if ((code == Op.PUT.getCode() || code == Op.PUTIFABSENT.getCode()) && !deletedList.contains(new KeyBytes(key2))) {
                    mergedList.add(kv2);
                    kv2 = getNextValue(kvKVIterator);
                    continue;
                }
                if (code == Op.DELETE.getCode()) {
//                    deletedList.add(new KeyBytes(key1));
//                    kv1 = getNextValue(localKVIterator);
                    mergedList.add(kv2);
                    kv2 = getNextValue(kvKVIterator);
                    continue;
                }
            }
            if (ByteArrayUtils.compare(key1, key2, pos, key1.length - 2) == 0) {
                if (code == Op.DELETE.getCode()) {
                    kv1 = getNextValue(localKVIterator);
                    kv2 = getNextValue(kvKVIterator);
                    continue;
                }
                if ((code == Op.PUT.getCode() || code == Op.PUTIFABSENT.getCode())) {
                    mergedList.add(kv1);
                    kv1 = getNextValue(localKVIterator);
                    kv2 = getNextValue(kvKVIterator);
                }
            }
        }
        while (kv1 != null) {
            if (!mergedList.contains(kv1) && (kv1.getKey()[kv1.getKey().length - 2] != Op.DELETE.getCode())) {
                mergedList.add(kv1);
            }
            kv1 = getNextValue(localKVIterator);
        }
        while (kv2 != null) {
            byte[] key = kv2.getKey();
            if (!mergedList.contains(kv2) && !deletedList.contains(new KeyBytes(key))) {
                mergedList.add(kv2);
            }
            kv2 = getNextValue(kvKVIterator);
        }

        return Iterators.transform(mergedList.iterator(), wrap(param.getCodec()::decode)::apply);
    }

    private KeyValue getNextValue(Iterator<KeyValue> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @AllArgsConstructor
    public static class KeyBytes {
        private final byte[] key;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyBytes keyBytes = (KeyBytes) o;
            return ByteArrayUtils.compare(this.key, keyBytes.key, 9, this.key.length - 2) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(key);
        }
    }
}
