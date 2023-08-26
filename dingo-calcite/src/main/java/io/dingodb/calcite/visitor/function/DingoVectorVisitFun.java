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

package io.dingodb.calcite.visitor.function;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.schema.DingoRootSchema;
import io.dingodb.calcite.schema.DingoSchema;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.PartVectorOperator;
import io.dingodb.exec.partition.DingoPartitionStrategyFactory;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.meta.MetaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public final class DingoVectorVisitFun {

    private DingoVectorVisitFun() {
    }

    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, DingoVector rel
    ) {
        RexCall rexCall = (RexCall) rel.getCall();

        String[] nameList = ((RexLiteral) rexCall.operands.get(0)).getValueAs(String.class).split("\\.");
        String schemaName = MetaService.DINGO_NAME;
        String tableName;
        if (nameList.length > 1) {
            schemaName = nameList[0];
            tableName = nameList[1];
        } else {
            tableName = nameList[0];
        }

        MetaService metaService = MetaService.root().getSubMetaService(schemaName);
        TableDefinition td = metaService.getTableDefinition(tableName);
        CommonId tableId = metaService.getTableId(tableName);

        NavigableMap<ComparableByteArray, RangeDistribution> ranges = metaService.getRangeDistribution(tableId);
        PartitionStrategy<CommonId, byte[]> ps = DingoPartitionStrategyFactory.createPartitionStrategy(td, ranges);

        String vectorColumnName = ((RexLiteral) rexCall.operands.get(1)).getValueAs(String.class);

        ImmutableList<RexNode> operands = ((RexCall) rexCall.operands.get(2)).operands;
        Float[] floatArray = new Float[operands.size()];
        for (int i = 0; i < operands.size(); i++) {
            floatArray[i] = ((Number) Objects.requireNonNull(((RexLiteral) operands.get(i)).getValue())).floatValue();
        }

        int topN = ((Number) Objects.requireNonNull(((RexLiteral) rexCall.operands.get(3)).getValue())).intValue();

        List<Output> outputs = new ArrayList<>();

        // Get all index table definition
        Map<CommonId, TableDefinition> indexDefinitions = metaService.getTableIndexDefinitions(tableName);
        for (Map.Entry<CommonId, TableDefinition> entry : indexDefinitions.entrySet()) {
            TableDefinition indexTableDefinition = entry.getValue();

            String indexType = indexTableDefinition.getProperties().get("indexType").toString();
            // Skip if not a vector table
            if (indexType.equals("scalar")) {
                continue;
            }

            List<String> indexColumns = indexTableDefinition.getColumns().stream().map(ColumnDefinition::getName)
                .collect(Collectors.toList());
            // Skip if the vector column is not included
            if (!indexColumns.contains(vectorColumnName.toUpperCase())) {
                continue;
            }

            CommonId indexTableId = entry.getKey();
            NavigableMap<ComparableByteArray, RangeDistribution> indexRangeDistribution =
                metaService.getIndexRangeDistribution(indexTableId);
            // Create tasks based on partitions
            for (RangeDistribution rangeDistribution : indexRangeDistribution.values()) {
                PartVectorOperator operator = new PartVectorOperator(
                    tableId,
                    rangeDistribution.id(),
                    td.getDingoType(),
                    td.getKeyMapping(),
                    null,
                    td.getMapping(),
                    td,
                    ps,
                    indexTableId,
                    rangeDistribution.id(),
                    floatArray,
                    topN
                );
                operator.setId(idGenerator.get());
                Task task = job.getOrCreate(currentLocation, idGenerator);
                task.putOperator(operator);
                outputs.addAll(operator.getOutputs());
            }
        }

        return outputs;
    }
}
