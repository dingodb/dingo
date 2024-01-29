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
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.schema.DingoSchema;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.common.CommonId;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.TableStatistic;
import io.dingodb.meta.entity.Table;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
public class DingoTable extends AbstractTable implements TranslatableTable {

    @Getter
    private final DingoParserContext context;
    @Getter
    private final List<String> names;

    private final TableStatistic tableStatistic;

    @Getter
    @EqualsAndHashCode.Include
    private final Table table;

    public DingoTable(
        @NonNull DingoParserContext context,
        @NonNull List<String> names,
        @NonNull TableStatistic tableStatistic,
        @NonNull Table table
    ) {
        super();
        this.context = context;
        this.names = names;
        this.tableStatistic = tableStatistic;
        this.table = table;
    }

    public CommonId getTableId() {
        return table.getTableId();
    }

    public DingoSchema getSchema() {
        return (DingoSchema) context.getSchemaByNames(names).schema;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DefinitionMapper.mapToRelDataType(table, typeFactory);
    }

    @Override
    public RelNode toRel(RelOptTable.@NonNull ToRelContext context, RelOptTable relOptTable) {
        return new LogicalDingoTableScan(
            context.getCluster(),
            context.getCluster().traitSet(),
            context.getTableHints(),
            relOptTable,
            null,
            null,
            null,
            null,
            null,
            ((DingoParserContext) context.getCluster().getPlanner().getContext()).isPushDown(),
                // && Optional.mapOrGet(table.engine, __ -> !__.contains("TXN"), () -> true),
            false
        );
    }

    @Override
    public Statistic getStatistic() {
        List<Integer> keyIndices = Arrays.stream(table.keyMapping().getMappings())
            .boxed()
            .collect(Collectors.toList());
        List<ImmutableBitSet> keys = ImmutableList.of(ImmutableBitSet.of(keyIndices));
        return new Statistic() {
            @Override
            public Double getRowCount() {
                return tableStatistic.getRowCount();
            }

            @Override
            public boolean isKey(ImmutableBitSet columns) {
                return keys.stream().allMatch(columns::contains);
            }

            @Override
            public List<ImmutableBitSet> getKeys() {
                return keys;
            }

            @Override
            public RelDistribution getDistribution() {
                return RelDistributions.hash(keyIndices);
            }
        };
    }

    @Override
    public <C> @Nullable C unwrap(@NonNull Class<C> clazz) {
        if (clazz.isAssignableFrom(InitializerExpressionFactory.class)) {
            return clazz.cast(DingoInitializerExpressionFactory.INSTANCE);
        } else if (clazz.isAssignableFrom(Prepare.PreparingTable.class)) {
            return clazz.cast(new DingoRelOptTable(this, context.getOption("user"),
                context.getOption("host")));
        }
        return super.unwrap(clazz);
    }
}
