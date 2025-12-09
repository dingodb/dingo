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

import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.visitor.DingoRelVisitor;
import io.dingodb.calcite.visitor.function.DingoTableSpoolVisitFun;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.ListTransientTable;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public class DingoTableSpool extends TableSpool implements DingoRel {
    @Getter
    private ListTransientTable listTransientTable;

    @Getter
    @Setter
    private List<RexNode> rexNodeList;

    public DingoTableSpool(
        RelOptCluster cluster, RelTraitSet traitSet, RelNode input, Type readType,
        Type writeType, RelOptTable table, ListTransientTable listTransientTable
    ) {
        super(cluster, traitSet, input, readType, writeType, table);
        this.listTransientTable = listTransientTable;
    }

    public static DingoTableSpool create(RelNode input, Type readType,
                                              Type writeType, RelOptTable table) {
        RelOptCluster cluster = input.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();
        RelTraitSet traitSet = cluster.traitSetOf(DingoConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> mq.collations(input))
            .replaceIf(RelDistributionTraitDef.INSTANCE,
                () -> mq.distribution(input));
        ListTransientTable tmp = table.unwrap(ListTransientTable.class);
        return new DingoTableSpool(cluster, traitSet, input, readType, writeType, table, tmp);
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    protected Spool copy(RelTraitSet relTraitSet, RelNode relNode, Type type, Type type1) {
        return null;
    }
}
