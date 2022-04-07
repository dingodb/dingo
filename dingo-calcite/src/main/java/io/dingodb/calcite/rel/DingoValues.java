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

package io.dingodb.calcite.rel;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.common.table.ElementSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DingoValues extends Values implements DingoRel {
    public DingoValues(
        RelOptCluster cluster,
        RelDataType rowType,
        ImmutableList<ImmutableList<RexLiteral>> tuples,
        RelTraitSet traits
    ) {
        super(cluster, rowType, tuples, traits);
    }

    public DingoValues(
        RelOptCluster cluster,
        RelDataType rowType,
        List<Object[]> tuples,
        RelTraitSet traits
    ) {
        this(cluster, rowType, toImmutableList(rowType, cluster.getRexBuilder(), tuples), traits);
    }

    private static ImmutableList<ImmutableList<RexLiteral>> toImmutableList(
        @Nonnull RelDataType rowType,
        RexBuilder rexBuilder,
        @Nonnull List<Object[]> tuples
    ) {
        List<RelDataTypeField> fields = rowType.getFieldList();
        return ImmutableList.copyOf(
            tuples.stream()
                .map(tuple -> ImmutableList.copyOf(
                    IntStream.range(0, fields.size())
                        .mapToObj(i -> rexBuilder.makeLiteral(tuple[i], fields.get(i).getType()))
                        .collect(Collectors.toList())
                ))
                .collect(Collectors.toList())
        );
    }

    @Nullable
    public static Object getValueOf(@Nonnull RexLiteral literal) {
        return ElementSchema.fromRelDataType(literal.getType()).convert(literal.getValue());
    }

    public List<Object[]> getValues() {
        return tuples.stream()
            .map(row -> row.stream()
                .map(DingoValues::getValueOf)
                .toArray(Object[]::new)
            ).collect(Collectors.toList());
    }

    @Override
    public <T> T accept(@Nonnull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
