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
import io.dingodb.common.type.ListType;
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
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.dingodb.exec.fun.vector.VectorCosineDistanceFun.cosine;
import static io.dingodb.exec.fun.vector.VectorIPDistanceFun.innerProduct;
import static io.dingodb.exec.fun.vector.VectorL2DistanceFun.l2DistanceCombine;
import static io.dingodb.exec.operator.TxnGetByKeysOperator.getLocalStore;

@Slf4j
public class TxnPartVectorOperator extends FilterProjectSourceOperator {

    public static final TxnPartVectorOperator INSTANCE = new TxnPartVectorOperator();

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        TxnPartVectorParam param = vertex.getParam();
        int vecIdx = param.getVectorIndex();
        String distanceType = param.getDistanceType();
        KeyValueCodec tableCodec;
        tableCodec = CodecService.getDefault().createKeyValueCodec(
            param.getTableDataSchema(), param.tableDataKeyMapping()
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
            Map<Integer, Integer> vecPriIdxMapping = getVecPriIdxMapping(param);
            for (VectorSearchResponse response : searchResponseList) {
                TxnVectorSearchResponse txnResponse = (TxnVectorSearchResponse) response;
                KeyValue tableData = new KeyValue(txnResponse.getTableKey(), txnResponse.getTableVal());
                Object[] vecTuples = tableCodec.decode(tableData);

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
                        Object[] objects = local.next();
                        if (vecIdx >= objects.length || distanceType == null) {
                            objects[objects.length - 1] = 0.0f;
                        } else {
                            Object ov = objects[vecIdx];
                            if (ov instanceof List) {
                                Object targetVector = Arrays.asList(param.getFloatArray());
                                float distance = 0.0f;
                                if (distanceType.contains("L2")) {
                                    distance = (float) l2DistanceCombine((List<Float>) ov, (List<Number>) targetVector);
                                } else if (distanceType.contains("INNER_PRODUCT")) {
                                    distance = (float) innerProduct((List<Float>) ov, (List<Float>) targetVector);
                                } else if (distanceType.contains("COSINE")) {
                                    distance = cosine((List<Float>) ov, targetVector);
                                }
                                objects[objects.length - 1] = distance;
                            } else {
                                objects[objects.length - 1] = 0.0;
                            }
                        }
                        results.add(objects);
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
                decode[vecIdx] = response.getFloatValues();

                vecPriIdxMapping.forEach((key, value) -> decode[value] = vecTuples[key]);
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

    private static Map<Integer, Integer> getVecPriIdxMapping(TxnPartVectorParam param) {
        int vecColSize = param.getTableDataColList().size();
        Map<Integer, Integer> mapping = new HashMap<>();
        for (int i = 0; i < vecColSize; i ++) {
            Column column = param.getTableDataColList().get(i);
            if (!column.isPrimary() && !(column.type instanceof ListType)) {
                int ix1 = param.getTable().getColumns().indexOf(column);
                mapping.put(i, ix1);
            }
        }
        return mapping;
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
            txnVecSelection[i] = result.map(param.getTableDataColList()::indexOf)
                .orElseThrow(() -> new RuntimeException("not found vector selection"));
        }
        return TupleMapping.of(txnVecSelection);
    }
}
