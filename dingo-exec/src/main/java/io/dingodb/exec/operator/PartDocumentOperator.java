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

import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.PartDocumentParam;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public final class PartDocumentOperator extends FilterProjectSourceOperator {
    public static final PartDocumentOperator INSTANCE = new PartDocumentOperator();

    private PartDocumentOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        PartDocumentParam param = vertex.getParam();
        StoreInstance instance = StoreService.getDefault().getInstance(param.getTableId(), param.getPartId());
        //TODO
        List<Object[]> results = new ArrayList<>();
        return results.iterator();
    }

}
