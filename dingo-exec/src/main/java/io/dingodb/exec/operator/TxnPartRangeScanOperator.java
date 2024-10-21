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
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.profile.SourceProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartRangeScanParam;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.exec.utils.TxnMergedIterator;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.ProfileScanIterator;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

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
        SourceProfile profile = param.getSourceProfile("txnPartRange");
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
            profile.setTaskType("executor");
        } else {
            localKVIterator = Iterators.transform(
                localStore.scan(new StoreInstance.Range(encodeStart, encodeEnd, includeStart, includeEnd), coprocessor),
                wrap(ByteUtils::mapping)::apply);
            kvKVIterator = kvStore.txnScan(param.getScanTs(), new StoreInstance.Range(startKey, endKey, includeStart, includeEnd), param.getTimeOut());
            profile.setTaskType("corp");
        }
        if (kvKVIterator instanceof ProfileScanIterator) {
            ProfileScanIterator profileScanIterator = (ProfileScanIterator) kvKVIterator;
            profile.getChildren().add(profileScanIterator.getInitRpcProfile());
        }
        profile.setRegionId(partId.seq);
        TxnMergedIterator txnMergedIterator = new TxnMergedIterator(localKVIterator, kvKVIterator, param.getCodec());
        profile.end();
        return txnMergedIterator;
    }

}
