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

import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Pair;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.DocumentPreFilterParam;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.DocumentSearchParameter;
import io.dingodb.store.api.transaction.data.DocumentWithScore;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
public class DocumentPreFilterOperator extends SoleOutOperator {

    public static final DocumentPreFilterOperator INSTANCE = new DocumentPreFilterOperator();
    public DocumentPreFilterOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        DocumentPreFilterParam param = vertex.getParam();
        param.setContext(context);
        param.getCache().add(tuple);
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        DocumentPreFilterParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("documentPreFilter");
        long start = System.currentTimeMillis();
        TupleMapping selection = param.getSelection();
        List<Object[]> cache = param.getCache();
        if (fin instanceof FinWithException) {
            edge.fin(fin);
            return;
        }
        Integer docIdIndex = param.getDocumentIdIndex();
        List<Long> rightList = cache.stream().map(e ->
            (Long) e[param.getDocumentIdIndex()]
        ).collect(Collectors.toList());

        if (rightList.isEmpty()) {
            edge.fin(fin);
            return;
        }

        String queStr = param.getQueryString();
        Integer topK = param.getTopK();
        StoreInstance instance = Services.KV_STORE.getInstance(param.getTable().getTableId(), param.getDistributions().firstEntry().getValue().getId());
        DocumentSearchParameter documentSearchParameter = DocumentSearchParameter.builder()
            .topN(topK)
            .documentIds(rightList)
            .queryString(queStr)
            .useIdFilter(true)
            .build();
        List<DocumentWithScore> documentWithScores = instance.documentSearch(
            param.getScanTs(),
            param.getIndexTableId(),
            documentSearchParameter);
        List<Pair<Long, Float>> docs = new ArrayList<>();
        for (DocumentWithScore document : documentWithScores) {
            if(document.getDocumentWithId().getDocument().getDocumentData() != null){
                float score = document.getScore();
                Pair<Long, Float> p = new Pair<>(document.getDocumentWithId().getId(), score);
                docs.add(p);
            }
        }

        for (int i = 0; i < cache.size(); i ++) {
            Object[] resTuple = new Object[param.getTable().columns.size() + 1];
            for(Pair<Long, Float> doc: docs) {
                if (cache.get(i)[docIdIndex] == doc.getKey()){
                    for(int k = 0; k < cache.get(i).length; k++){
                        resTuple[k] = cache.get(i)[k];
                    }
                    resTuple[resTuple.length -1] = doc.getValue();
                    edge.transformToNext(param.getContext(), selection.revMap(resTuple));
                }
            }
        }
        param.clear();
        profile.time(start);
        edge.fin(fin);
    }
}
