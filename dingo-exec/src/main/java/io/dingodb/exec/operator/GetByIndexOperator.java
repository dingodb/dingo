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
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.GetByIndexParam;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static io.dingodb.common.util.Utils.calculatePrefixCount;

@Slf4j
public final class GetByIndexOperator extends FilterProjectSourceOperator {
    public static final GetByIndexOperator INSTANCE = new GetByIndexOperator();

    private GetByIndexOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        GetByIndexParam param = vertex.getParam();
        List<Iterator<Object[]>> iteratorList = scan(param);
        List<Object[]> objectList = new ArrayList<>();
        for (Iterator<Object[]> iterator : iteratorList) {
            while (iterator.hasNext()) {
                Object[] objects = iterator.next();
                if (param.isLookup()) {
                    Object[] val = lookUp(objects, param);
                    if (val != null) {
                        objectList.add(val);
                    }
                } else {
                    objectList.add(transformTuple(objects, param));
                }
            }
        }
        return objectList.iterator();
    }

    private List<Iterator<Object[]>> scan(GetByIndexParam param) {
        try {
            List<Iterator<Object[]>> iteratorList = new ArrayList<>();
            for (Object[] tuple : param.getIndexValues()) {
                byte[] keys = param.getCodec().encodeKeyPrefix(tuple, calculatePrefixCount(tuple));
                iteratorList.add(param.getPart().scan(keys));
            }
            return iteratorList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Object[] lookUp(Object[] tuples, GetByIndexParam param) {
        TupleMapping indices = param.getKeyMapping();
        TableDefinition tableDefinition = param.getTableDefinition();
        Object[] keyTuples = new Object[tableDefinition.getColumnsCount()];
        for (int i = 0; i < indices.getMappings().length; i ++) {
            keyTuples[indices.get(i)] = tuples[i];
        }
        try {
            byte[] keys = param.getLookupCodec().encodeKey(keyTuples);
            CommonId regionId = PartitionService.getService(
                    Optional.ofNullable(tableDefinition.getPartDefinition())
                        .map(PartitionDefinition::getFuncName)
                        .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
                    .calcPartId(keys, param.getRanges());
            StoreInstance storeInstance = StoreService.getDefault().getInstance(param.getTableId(), regionId);
            return param.getLookupCodec().decode(storeInstance.get(keys));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Object[] transformTuple(Object[] tuple, GetByIndexParam param) {
        TupleMapping selection = param.getSelection();
        TableDefinition indexDefinition = param.getIndexDefinition();
        TableDefinition tableDefinition = param.getTableDefinition();
        Object[] response = new Object[tableDefinition.getColumnsCount()];
        List<Integer> selectedColumns = mapping(selection, tableDefinition, indexDefinition);
        for (int i = 0; i < selection.size(); i ++) {
            response[selection.get(i)] = tuple[selectedColumns.get(i)];
        }
        return response;
    }

    private static List<Integer> mapping(TupleMapping selection, TableDefinition td, TableDefinition index) {
        Integer[] mappings = new Integer[selection.size()];
        for (int i = 0; i < selection.size(); i ++) {
            ColumnDefinition columnDefinition = td.getColumn(selection.get(i));
            mappings[i] = index.getColumnIndex(columnDefinition.getName());
        }
        return Arrays.asList(mappings);
    }

}
