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

package io.dingodb.calcite.operation;

import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.MetaService;
import io.dingodb.partition.DingoPartitionServiceProvider;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;

public class ShowTableDistributionOperation implements QueryOperation {

    @Setter
    public SqlNode sqlNode;

    private MetaService metaService;

    private String tableName;

    public ShowTableDistributionOperation(SqlNode sqlNode, String tableName) {
        this.sqlNode = sqlNode;
        metaService = MetaService.root().getSubMetaService(MetaServiceUtils.getSchemaName(tableName));
        this.tableName = tableName;
    }

    @Override
    public Iterator getIterator() {
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
        CommonId tableId = metaService.getTableId(tableName);
        if (tableId == null) {
            throw new RuntimeException("Table " + tableName + " doesn't exist");
        }

        TableDefinition tableDefinition = metaService.getTableDefinition(tableId);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
            = metaService.getRangeDistribution(tableId);
        List<List<String>> regionList = new ArrayList<>();

        List<Integer> keyColumnIndices = tableDefinition.getKeyColumnIndices();
        List<RangeDistribution> ranges = rangeDistribution.values().stream().collect(Collectors.toList());
        String partName = Optional.ofNullable(tableDefinition.getPartDefinition().getFuncName())
            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME);
        boolean hashPartition = partName.toLowerCase().equals("hash");
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
                row.set(row.size() - 3, String.format("%s [ %s, %s )", row.get(row.size() - 3), row.get(row.size() - 2), row.get(row.size() - 1)));
                row.remove(row.size() - 2);
            } else {
                // [key, key)
                row.set(row.size() - 2, String.format("[ %s, %s )", row.get(row.size() - 2), row.get(row.size() - 1)));
            }
            row.remove(row.size() - 1);
        });
        return regionList;
    }

    private String buildKeyStr(List<Integer> keyColumnIndices, Object[] start) {
        if (start == null) {
            return "Infinity";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; ; i++) {
            Object object;
            if (i >= keyColumnIndices.size() || (object = start[keyColumnIndices.get(i)]) == null) {
                if (i == 0) {
                    builder.append("Infinity");
                } else {
                    builder.append(")");
                }
                break;
            }

            if (i == 0) {
                builder.append("Key(");
            } else {
                builder.append(", ");
            }
            builder.append(object);
        }
        return builder.toString();
    }
}
