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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.TxnPartDocumentParam;
import io.dingodb.meta.entity.Column;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.transaction.data.DocumentSearchParameter;
import io.dingodb.store.api.transaction.data.DocumentValue;
import io.dingodb.store.api.transaction.data.DocumentWithScore;
import io.dingodb.store.api.transaction.data.ScalarField;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.dingodb.exec.operator.TxnGetByKeysOperator.getLocalStore;

@Slf4j
public class TxnPartDocumentOperator extends FilterProjectSourceOperator {

    public static final TxnPartDocumentOperator INSTANCE = new TxnPartDocumentOperator();

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        TxnPartDocumentParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("partDocument");
        long start = System.currentTimeMillis();
        StoreInstance instance = Services.KV_STORE.getInstance(param.getTableId(), param.getPartId());
        DocumentSearchParameter documentSearchParameter = DocumentSearchParameter.builder()
            .topN(param.getTopN())
            .queryString(param.getQueryString())
            .build();
        List<DocumentWithScore> documentWithScores = instance.documentSearch(
            param.getScanTs(),
            param.getIndexId(),
            documentSearchParameter);
        List<Object[]> results = new ArrayList<>();
        List<Column> columns = param.getTable().getColumns();
        Object[] priTuples = new Object[param.getTable().columns.size() + 1];
        for (DocumentWithScore document : documentWithScores) {
            if (document.getDocumentWithId().getDocument() != null) {
                if (document.getDocumentWithId().getDocument().getTableData() != null) {
                    KeyValue tableData = new KeyValue(
                        document.getDocumentWithId().getDocument().getTableData().getTableKey(),
                        document.getDocumentWithId().getDocument().getTableData().getTableValue());
                    byte[] tmp1 = new byte[tableData.getKey().length];
                    System.arraycopy(tableData.getKey(), 0, tmp1, 0, tableData.getKey().length);
                    CommonId regionId = PartitionService.getService(
                            Optional.ofNullable(param.getTable().getPartitionStrategy())
                                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
                        .calcPartId(tableData.getKey(), param.getDistributions());
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
                            if (objects[priTuples.length - 1] instanceof Float) {
                                results.add(objects);
                            }
                        }
                        continue;
                    }
                    StoreInstance storeInstance = StoreService.getDefault().getInstance(param.getTableId(), regionId);
                    KeyValue keyValue = storeInstance.txnGet(param.getScanTs(), tableData.getKey(), param.getTimeOut());
                    if (keyValue == null || keyValue.getValue() == null) {
                        continue;
                    }
                    Object[] decode = param.getCodec().decode(keyValue);
                    decode[decode.length - 1] = document.getScore();
                    results.add(decode);
                } else {
                    Map<String, DocumentValue> documentData = document.getDocumentWithId()
                        .getDocument().getDocumentData();
                    Set<Map.Entry<String, DocumentValue>> entries = documentData.entrySet();
                    for (Map.Entry<String, DocumentValue> entry : entries) {
                        String key = entry.getKey();
                        DocumentValue value = entry.getValue();
                        ScalarField fieldValue = value.getFieldValue();
                        int idx = 0;
                        for (int i = 0; i < columns.size(); i++) {
                            if (columns.get(i).getName().equals(key)) {
                                idx = i;
                                break;
                            }
                        }
                        priTuples[idx] = fieldValue.getData();
                    }

                    float score = document.getScore();
                    priTuples[priTuples.length - 1] = score;
                    results.add(priTuples);
                }
            } else {
                LogUtils.error(log, ("Failed to get document"));
            }
        }
        profile.incrTime(start);
        return results.iterator();
    }

    //private static Map<Integer, Integer> getDocPriIdxMapping(TxnPartDocumentParam param) {
    //    int docColSize = param.getTableDataColList().size();
    //    Map<Integer, Integer> mapping = new HashMap<>();
    //    for (int i = 0; i < docColSize; i ++) {
    //        Column column = param.getTableDataColList().get(i);
    //        if (!column.isPrimary()) {
    //            int ix1 = param.getTable().getColumns().indexOf(column);
    //            mapping.put(i, ix1);
    //        }
    //    }
    //    return mapping;
    //}
}
