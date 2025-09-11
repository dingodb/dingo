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

package io.dingodb.calcite.executor;

import io.dingodb.calcite.grammar.dql.SqlShowTableIndexRegions;
import io.dingodb.common.mysql.DingoErrUtil;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.common.mysql.error.ErrorCode.ErrNoSuchTable;
import static io.dingodb.common.mysql.error.ErrorCode.ErrWrongNameForIndex;
import static io.dingodb.common.util.Utils.buildKeyStr;

public class ShowTableIndexRegionsExecutor extends QueryExecutor {
    private final String tableName;

    private final String schemaName;

    private final String indexName;

    public ShowTableIndexRegionsExecutor(SqlShowTableIndexRegions indexRegions) {
        this.tableName = indexRegions.tableName;
        this.schemaName = indexRegions.schemaName;
        this.indexName = indexRegions.indexName;
    }

    @Override
    Iterator<Object[]> getIterator() {
        List<Object[]> tuples = new ArrayList<>();
        List<List<String>> distributions = getDistributions();
        for (List<String> values : distributions) {
            Object[] tuple = values.toArray();
            tuples.add(tuple);
        }
        return tuples.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("Id");
        columns.add("Type");
        columns.add("Value");
        return columns;
    }

    private List<List<String>> getDistributions() {
        InfoSchema is = DdlService.root().getIsLatest();
        Table table = is.getTable(schemaName, tableName);
        if (table == null) {
            throw DingoErrUtil.newStdErr(ErrNoSuchTable, tableName);
        }
        List<IndexTable> indexList = table.getIndexes().stream()
            .filter(indexTable -> indexTable.getName().equalsIgnoreCase(indexName)).toList();
        if (indexList.size() != 1) {
            throw DingoErrUtil.newStdErr(ErrWrongNameForIndex, indexName);
        }
        IndexTable indexTable = indexList.get(0);
        MetaService metaService = MetaService.root().getSubMetaService(schemaName);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
            = metaService.getRangeDistribution(indexTable.tableId);
        List<List<String>> regionList = new ArrayList<>();

        TupleMapping keyColumnIndices = indexTable.keyMapping();
        List<RangeDistribution> ranges = new ArrayList<>(rangeDistribution.values());
        String partName = Optional.ofNullable(indexTable.getPartitionStrategy())
            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME);
        boolean hashPartition = partName.equalsIgnoreCase("hash");
        for (int i = 0; i < ranges.size(); i++) {
            RangeDistribution range = ranges.get(i);
            List<String> rangeValues = new ArrayList<>();

            rangeValues.add(range.getId().toString());
            rangeValues.add(partName);
            String key = buildKeyStr(keyColumnIndices, range.getStart());
            // hash(partid)
            if (hashPartition) {
                rangeValues.add(partName.toLowerCase() + "(" + range.getId().domain + ") ");
            }
            rangeValues.add(key);

            if (i + 1 < ranges.size()) {
                key = buildKeyStr(keyColumnIndices, ranges.get(i + 1).getStart());
            } else {
                key = buildKeyStr(keyColumnIndices, null);
            }
            rangeValues.add(key);

            regionList.add(rangeValues);
        }

        regionList.forEach(row -> {
            if (hashPartition) {
                // hash(partid) [key, key)
                row.set(row.size() - 3, String.format("%s [ %s, %s )", row.get(row.size() - 3),
                    row.get(row.size() - 2), row.get(row.size() - 1)));
                row.remove(row.size() - 2);
            } else {
                // [key, key)
                row.set(row.size() - 2, String.format("[ %s, %s )", row.get(row.size() - 2), row.get(row.size() - 1)));
            }
            row.remove(row.size() - 1);
        });
        return regionList;
    }
}
