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

package io.dingodb.calcite.rel.logical;

import io.dingodb.calcite.DingoRelOptTable;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

public class LogicalFunctionScan extends TableFunctionScan {
    @Getter
    private final DingoRelOptTable table;
    @Getter
    private final List<SqlNode> operands;

    public LogicalFunctionScan(RelOptCluster cluster,
                               RelTraitSet traitSet, List<RelNode> inputs, RexNode rexCall,
                               @Nullable Type elementType, RelDataType rowType,
                               @Nullable Set<RelColumnMapping> columnMappings,
                               DingoRelOptTable table,
                               List<SqlNode> operands
    ) {
        super(cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings);
        this.table = table;
        this.operands = operands;
    }

    @Override
    public TableFunctionScan copy(RelTraitSet traitSet, List<RelNode> inputs, RexNode rexCall,
        @Nullable Type elementType, RelDataType rowType, @Nullable Set<RelColumnMapping> columnMappings
    ) {
        return null;
    }
}
