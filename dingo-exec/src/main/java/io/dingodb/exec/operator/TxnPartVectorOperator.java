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
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Optional;
import io.dingodb.common.vector.TxnVectorSearchResponse;
import io.dingodb.common.vector.VectorSearchResponse;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.TxnPartVectorParam;
import io.dingodb.meta.entity.Column;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.dingodb.exec.operator.TxnGetByKeysOperator.getLocalStore;

public class TxnPartVectorOperator extends FilterProjectSourceOperator {

    public static final TxnPartVectorOperator INSTANCE = new TxnPartVectorOperator();

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        TxnPartVectorParam param = vertex.getParam();
        KeyValueCodec tableCodec;
        tableCodec = CodecService.getDefault().createKeyValueCodec(
            param.getTableDataSchema(), param.getTable().keyMapping()
        );
        StoreInstance instance = Services.KV_STORE.getInstance(param.getTableId(), param.getPartId());
        List<VectorSearchResponse> searchResponseList = instance.vectorSearch(
            param.getScanTs(),
            param.getIndexId(),
            param.getFloatArray(),
            param.getTopN(),
            param.getParameterMap(),
            param.getCoprocessor());
        List<Object[]> results = new ArrayList<>();
        if (param.isLookUp()) {
            for (VectorSearchResponse response : searchResponseList) {
                TxnVectorSearchResponse txnResponse = (TxnVectorSearchResponse) response;

                byte[] tmp1 = new byte[txnResponse.getKey().length];
                System.arraycopy(txnResponse.getKey(), 0, tmp1, 0, txnResponse.getKey().length);
                CommonId regionId = PartitionService.getService(
                        Optional.ofNullable(param.getTable().getPartitionStrategy())
                            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
                    .calcPartId(txnResponse.getKey(), param.getDistributions());
                CodecService.getDefault().setId(tmp1, regionId.domain);
                CommonId txnId = vertex.getTask().getTxnId();
                Iterator<Object[]> local = getLocalStore(
                    regionId,
                    param.getCodec(),
                    tmp1,
                    param.getTableId(),
                    txnId,
                    regionId.encode(),
                    vertex.getTask().getTransactionType()
                    );
                if (local != null) {
                    while (local.hasNext()) {
                        results.add(local.next());
                    }
                    continue;
                }

                StoreInstance storeInstance = StoreService.getDefault().getInstance(param.getTableId(), regionId);
                KeyValue keyValue = storeInstance.txnGet(param.getScanTs(), txnResponse.getKey(), param.getTimeOut());
                if (keyValue == null || keyValue.getValue() == null) {
                    continue;
                }
                Object[] decode = param.getCodec().decode(keyValue);
                decode[decode.length - 1] = response.getDistance();
                results.add(decode);
            }
        } else {
            TupleMapping vecSelection = mapping2VecSelection(param);
            for (VectorSearchResponse response : searchResponseList) {
                TxnVectorSearchResponse txnResponse = (TxnVectorSearchResponse) response;
                KeyValue tableData = new KeyValue(txnResponse.getTableKey(), txnResponse.getTableVal());
                Object[] tuples = tableCodec.decode(tableData);
                Object[] priTuples = new Object[param.getTable().columns.size() + 1];
                TupleMapping resultSec = param.getResultSelection();
                for (int i = 0; i < resultSec.size(); i ++) {
                    priTuples[resultSec.get(i)] = tuples[vecSelection.get(i)];
                }
                priTuples[priTuples.length - 1] = response.getDistance();
                results.add(priTuples);
            }
        }
        return results.iterator();
    }

    private static TupleMapping mapping2VecSelection(TxnPartVectorParam param) {
        TupleMapping selection = param.getResultSelection();

        int[] txnVecSelection = new int[selection.size()];
        for (int i = 0; i < selection.size(); i ++) {
            if (selection.get(i) == param.getTable().columns.size()) {
                continue;
            }
            String name = param.getTable().getColumns().get(selection.get(i)).getName();
            java.util.Optional<Column> result = param.getIndexTable().getColumns()
                .stream()
                .filter(element -> element.getName().equalsIgnoreCase(name))
                .findFirst();
            txnVecSelection[i] = result.map(param.getIndexTable().getColumns()::indexOf)
                .orElseThrow(() -> new RuntimeException("not found vector selection"));
        }
        return TupleMapping.of(txnVecSelection);
    }
}
