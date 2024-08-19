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
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.DocumentSearchParameter;
import io.dingodb.store.api.transaction.data.DocumentWithScore;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        DocumentSearchParameter documentSearchParameter = DocumentSearchParameter.builder()
            .topN(param.getTopN())
            .queryString(param.getQueryString())
            .build();
        List<DocumentWithScore> documentWithScores = instance.documentSearch(
            param.getScanTs(),
            param.getIndexId(),
            documentSearchParameter);
        List<Object[]> results = new ArrayList<>();
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

//    private static TupleMapping mapping2DocSelection(TxnPartDocumentParam param) {
//        TupleMapping selection = param.getResultSelection();
//
//        int[] txnDocSelection = new int[selection.size()];
//        for (int i = 0; i < selection.size(); i ++) {
//            if (selection.get(i) == param.getTable().columns.size()) {
//                continue;
//            }
//            String name = param.getTable().getColumns().get(selection.get(i)).getName();
//            java.util.Optional<Column> result = param.getIndexTable().getColumns()
//                .stream()
//                .filter(element -> element.getName().equalsIgnoreCase(name))
//                .findFirst();
//            txnDocSelection[i] = result.map(param.getTableDataColList()::indexOf)
//                .orElseThrow(() -> new RuntimeException("not found document selection"));
//        }
//        return TupleMapping.of(txnDocSelection);
//    }
}
