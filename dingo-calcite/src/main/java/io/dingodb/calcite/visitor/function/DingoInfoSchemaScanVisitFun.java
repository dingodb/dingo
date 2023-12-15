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

import io.dingodb.calcite.rel.DingoInfoSchemaScan;
import io.dingodb.calcite.utils.SqlExprUtils;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.InfoSchemaScanOperator;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

public final class DingoInfoSchemaScanVisitFun {

    private DingoInfoSchemaScanVisitFun() {
    }

    public static List<Output> visit(
        Job job,
        IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        @NonNull DingoInfoSchemaScan rel
    ) {
        final TableDefinition td = TableUtils.getTableDefinition(rel.getTable());
        SqlExpr filter = null;
        if (rel.getFilter() != null) {
            filter = SqlExprUtils.toSqlExpr(rel.getFilter());
        }
        String tableName;
        if (rel.getTable().getQualifiedName() != null && rel.getTable().getQualifiedName().size() > 2) {
            tableName = rel.getTable().getQualifiedName().get(2);
        } else {
            tableName = td.getName();
        }
        InfoSchemaScanOperator operator = new InfoSchemaScanOperator(
            td.getDingoType(),
            filter,
            rel.getSelection(),
            tableName
        );
        Task task = job.getOrCreate(currentLocation, idGenerator);
        operator.setId(idGenerator.getOperatorId(task.getId()));
        task.putOperator(operator);

        return new ArrayList<>(operator.getOutputs());
    }
}
