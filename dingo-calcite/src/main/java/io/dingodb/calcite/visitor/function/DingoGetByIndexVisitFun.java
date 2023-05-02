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

import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.GetByIndexOperator;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.LinkedList;
import java.util.List;

public final class DingoGetByIndexVisitFun {
    private DingoGetByIndexVisitFun() {
    }

    @NonNull
    public static LinkedList<Output> visit(
        Job job, IdGenerator idGenerator, Location currentLocation, DingoJobVisitor visitor, @NonNull DingoGetByIndex rel
    ) {
        final CommonId tableId = MetaServiceUtils.getTableId(rel.getTable());
        final TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        Index index = td.getIndex(rel.getIndexName());
        TupleMapping mapping = TupleMapping.of(td.getColumnIndices(index.getColumns()));
        List<Object[]> keyTuples = TableUtils.getTuplesForMapping(rel.getPoints(), td, mapping);
        GetByIndexOperator operator = new GetByIndexOperator(tableId, td.getDingoType(), mapping, keyTuples,
            SqlExprUtils.toSqlExpr(rel.getFilter()), rel.getSelection()
        );
        operator.setId(idGenerator.get());
        Task task = job.getOrCreate(currentLocation, idGenerator);
        task.putOperator(operator);
        return new LinkedList<>(operator.getOutputs());
    }
}
