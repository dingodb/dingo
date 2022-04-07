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

package io.dingodb.calcite;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.common.table.TableDefinition;
import lombok.Getter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.annotation.Nonnull;

public class DingoTable extends AbstractTable implements TranslatableTable {
    @Getter
    private final TableDefinition tableDefinition;

    protected DingoTable(TableDefinition tableDefinition) {
        this.tableDefinition = tableDefinition;
    }

    public static DingoTable dingo(Table table) {
        return (DingoTable) table;
    }

    public static DingoTable dingo(@Nonnull RelOptTable table) {
        return table.unwrap(DingoTable.class);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return tableDefinition.getRelDataType(typeFactory);
    }

    @Override
    public RelNode toRel(@Nonnull RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new DingoTableScan(
            context.getCluster(),
            context.getCluster().traitSet().replace(DingoConventions.DINGO),
            context.getTableHints(),
            relOptTable
        );
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(
            100.0,
            // Only the primary keys are unique keys.
            ImmutableList.of(ImmutableBitSet.of(tableDefinition.getKeyMapping().getMappings()))
        );
    }

    @Override
    public <C> @Nullable C unwrap(@Nonnull Class<C> clazz) {
        if (clazz.isInstance(DingoInitializerExpressionFactory.INSTANCE)) {
            return clazz.cast(DingoInitializerExpressionFactory.INSTANCE);
        }
        return super.unwrap(clazz);
    }
}
