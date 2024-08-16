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
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.ListType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Optional;
import io.dingodb.common.document.TxnDocumentSearchResponse;
import io.dingodb.common.document.DocumentSearchResponse;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.TxnPartDocumentParam;
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

import static io.dingodb.exec.operator.TxnGetByKeysOperator.getLocalStore;

@Slf4j
public class TxnPartDocumentOperator extends FilterProjectSourceOperator {

    public static final TxnPartDocumentOperator INSTANCE = new TxnPartDocumentOperator();

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        TxnPartDocumentParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("partDocument");
        long start = System.currentTimeMillis();
        int vecIdx = param.getDocumentIndex();
        KeyValueCodec tableCodec;
        tableCodec = CodecService.getDefault().createKeyValueCodec(
            param.getTable().version, param.getTableDataSchema(), param.tableDataKeyMapping()
        );
        StoreInstance instance = Services.KV_STORE.getInstance(param.getTableId(), param.getPartId());
        List<DocumentSearchResponse> searchResponseList = instance.documentSearch(
            param.getScanTs(),
            param.getIndexId(),
            param.getKeyword(),
            param.getTopN(),
            param.getParameterMap(),
            param.getCoprocessor());
        List<Object[]> results = new ArrayList<>();
        if (param.isLookUp()) {
            Map<Integer, Integer> vecPriIdxMapping = getDocPriIdxMapping(param);
            for (DocumentSearchResponse response : searchResponseList) {
                TxnDocumentSearchResponse txnResponse = (TxnDocumentSearchResponse) response;
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
            TupleMapping vecSelection = mapping2DocSelection(param);
            for (DocumentSearchResponse response : searchResponseList) {
                TxnDocumentSearchResponse txnResponse = (TxnDocumentSearchResponse) response;
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
        profile.incrTime(start);
        return results.iterator();
    }

    private static Map<Integer, Integer> getDocPriIdxMapping(TxnPartDocumentParam param) {
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

    private static TupleMapping mapping2DocSelection(TxnPartDocumentParam param) {
        TupleMapping selection = param.getResultSelection();

        int[] txnDocSelection = new int[selection.size()];
        for (int i = 0; i < selection.size(); i ++) {
            if (selection.get(i) == param.getTable().columns.size()) {
                continue;
            }
            String name = param.getTable().getColumns().get(selection.get(i)).getName();
            java.util.Optional<Column> result = param.getIndexTable().getColumns()
                .stream()
                .filter(element -> element.getName().equalsIgnoreCase(name))
                .findFirst();
            txnDocSelection[i] = result.map(param.getTableDataColList()::indexOf)
                .orElseThrow(() -> new RuntimeException("not found document selection"));
        }
        return TupleMapping.of(txnDocSelection);
    }
}
