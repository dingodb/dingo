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

import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.common.type.DingoType;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.stream.Collectors;

public class LogicalDingoValues extends AbstractRelNode {
    @Getter
    private final List<Object[]> tuples;
    private final RelDataType rowType;

    public LogicalDingoValues(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelDataType rowType,
        List<Object[]> tuples
    ) {
        super(cluster, traits);
        this.rowType = rowType;
        this.tuples = tuples;
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public @NonNull RelWriter explainTerms(@NonNull RelWriter pw) {
        super.explainTerms(pw);
        DingoType type = DefinitionMapper.mapToDingoType(rowType);
        pw.item("tuples", tuples.stream().map(type::format).collect(Collectors.joining(", ")));
        return pw;
    }
}
