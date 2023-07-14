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

import io.dingodb.calcite.rel.DingoFunctionScan;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.PartRangeScanOperator;
import io.dingodb.meta.MetaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

@Slf4j
public final class DingoFunctionScanVisitFun {

    private DingoFunctionScanVisitFun() {
    }

    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, DingoFunctionScan rel
    ) {
        RexCall rexCall = (RexCall) rel.getCall();

        String[] nameList = ((RexLiteral) rexCall.operands.get(0)).getValueAs(String.class).split("\\.");
        String schemaName = "DINGO";
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

        List<Output> outputs = new ArrayList<>();

        for (RangeDistribution rd : ranges.values()) {
            PartRangeScanOperator operator = new PartRangeScanOperator(
                tableId,
                rd.id(),
                td.getDingoType(),
                td.getKeyMapping(),
                null,
                td.getMapping(),
                rd.getStartKey(),
                rd.getEndKey(),
                rd.isWithStart(),
                rd.isWithEnd(),
                null,
                null,
                td.getDingoType(),
                false
            );
            operator.setId(idGenerator.get());
            Task task = job.getOrCreate(currentLocation, idGenerator);
            task.putOperator(operator);
            outputs.addAll(operator.getOutputs());
        }

        return outputs;
    }
}
