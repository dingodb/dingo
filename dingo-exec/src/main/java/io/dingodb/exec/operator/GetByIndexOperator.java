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
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.GetByIndexParam;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.common.util.Utils.calculatePrefixCount;

@Slf4j
public final class GetByIndexOperator extends FilterProjectOperator {
    public static final GetByIndexOperator INSTANCE = new GetByIndexOperator();

    private GetByIndexOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Context context, Object[] tuple, Vertex vertex) {
        GetByIndexParam param = vertex.getParam();
        StoreInstance store = Services.KV_STORE.getInstance(param.getIndexTableId(), context.getDistribution().getId());
        byte[] keys = param.getCodec().encodeKeyPrefix(tuple, calculatePrefixCount(tuple));
        Iterator<Object[]> iterator = Iterators.transform(
            store.scan(System.identityHashCode(keys), keys),
            wrap(param.getCodec()::decode)::apply
        );
        List<Object[]> objectList = new ArrayList<>();
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

        return objectList.iterator();
    }

    private static Object[] lookUp(Object[] tuples, GetByIndexParam param) {
        TupleMapping indices = param.getKeyMapping();
        Table tableDefinition = param.getTable();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
            MetaService.root().getRangeDistribution(tableDefinition.tableId);
        Object[] keyTuples = new Object[tableDefinition.getColumns().size()];
        for (int i = 0; i < indices.getMappings().length; i ++) {
            keyTuples[indices.get(i)] = tuples[i];
        }
        byte[] keys = param.getLookupCodec().encodeKey(keyTuples);
        CommonId regionId = PartitionService.getService(
                Optional.ofNullable(tableDefinition.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
                .calcPartId(keys, ranges);
        StoreInstance storeInstance = StoreService.getDefault().getInstance(param.getTableId(), regionId);
        return param.getLookupCodec().decode(storeInstance.get(keys));
    }

    private static Object[] transformTuple(Object[] tuple, GetByIndexParam param) {
        TupleMapping selection = param.getSelection();
        Table index = param.getIndex();
        Table table = param.getTable();
        Object[] response = new Object[table.getColumns().size()];
        List<Integer> selectedColumns = mapping(selection, table, index);
        for (int i = 0; i < selection.size(); i ++) {
            response[selection.get(i)] = tuple[selectedColumns.get(i)];
        }
        return response;
    }

    private static List<Integer> mapping(TupleMapping selection, Table td, Table index) {
        Integer[] mappings = new Integer[selection.size()];
        for (int i = 0; i < selection.size(); i ++) {
            Column column = td.getColumns().get(selection.get(i));
            mappings[i] = index.getColumns().indexOf(column);
        }
        return Arrays.asList(mappings);
    }

}
