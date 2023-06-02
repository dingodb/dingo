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
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteUtils;
import io.dingodb.meta.MetaService;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

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
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(tableId, tableDefinition);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
            = metaService.getRangeDistribution(tableId);
        List<List<String>> regionList = new ArrayList<>();

        Iterator<RangeDistribution> iterator = rangeDistribution.values().iterator();
        while (iterator.hasNext()) {
            RangeDistribution range = iterator.next();
            List<String> rangeValues = new ArrayList<>();

            rangeValues.add(range.getId().toString());
            rangeValues.add("Range");

            // Value like [ Key(1, a), key(2, a) )
            StringBuilder builder = new StringBuilder("[ ");
            builder.append(ByteUtils.byteArrayToHexString(range.getStartKey()));
            builder.append(", ");
            builder.append(ByteUtils.byteArrayToHexString(range.getEndKey()));
            builder.append(" )");
            rangeValues.add(builder.toString());

            regionList.add(rangeValues);
        }

        return regionList;
    }
}
