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

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.TxnPartDeleteParam;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

public class TxnPartDeleteOperator extends PartModifyOperator {
    public final static TxnPartDeleteOperator INSTANCE = new TxnPartDeleteOperator();

    private TxnPartDeleteOperator() {
    }

    @Override
    protected boolean pushTuple(Object[] tuple, Vertex vertex) {
        TxnPartDeleteParam param = vertex.getParam();
        byte[] keys = wrap(param.getCodec()::encodeKey).apply(tuple);
        CommonId tableId = param.getTableId();
        CommonId partId = PartitionService.getService(
                Optional.ofNullable(param.getTableDefinition().getPartDefinition())
                    .map(PartitionDefinition::getFuncName)
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keys, param.getDistributions());
        StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, partId);
        byte[] txnIdBytes = vertex.getTask().getTxnId().encode();
        byte[] tableIdBytes = tableId.encode();
        byte[] partIdBytes = partId.encode();
        byte[] resultKeys = ByteUtils.encode(
            keys,
            Op.DELETE.getCode(),
            (txnIdBytes.length + tableIdBytes.length + partIdBytes.length),
            txnIdBytes, tableIdBytes, partIdBytes);
        KeyValue keyValue = new KeyValue(resultKeys, null);
        if (store.put(keyValue)) {
            param.inc();
        }
        return true;
    }
}
