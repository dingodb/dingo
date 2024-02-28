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

import io.dingodb.calcite.visitor.DingoRelVisitor;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.TimeZone;

public class DingoExportData extends SingleRel implements DingoRel {

    @Getter
    private final String outfile;

    @Getter
    private final byte[] terminated;

    @Getter
    private final String statementId;

    @Getter
    private final String enclosed;

    @Getter
    private final byte[] lineTerminated;

    @Getter
    private final byte[] escaped;

    @Getter
    private final String charset;

    @Getter
    private final byte[] lineStarting;

    @Getter
    private final TimeZone timeZone;

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits  traits
     * @param input   Input relational expression
     */
    public DingoExportData(RelOptCluster cluster,
                           RelTraitSet traits,
                           RelNode input,
                           String outfile,
                           byte[] terminated,
                           String statementId,
                           String enclosed,
                           byte[] lineTerminated,
                           byte[] escaped,
                           String charset,
                           byte[] lineStarting,
                           TimeZone timeZone) {
        super(cluster, traits, input);
        this.outfile = outfile;
        this.terminated = terminated;
        this.statementId = statementId;
        this.enclosed = enclosed;
        this.lineTerminated = lineTerminated;
        this.escaped = escaped;
        this.charset = charset;
        this.lineStarting = lineStarting;
        this.timeZone = timeZone;
    }

    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return 1;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoExportData(
            getCluster(),
            traitSet,
            sole(inputs),
            outfile,
            terminated,
            statementId,
            enclosed,
            lineTerminated,
            escaped,
            charset,
            lineStarting,
            timeZone);
    }
}
