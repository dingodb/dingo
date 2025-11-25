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

package io.dingodb.calcite.rule.dingo;

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.rel.DingoWindow;
import io.dingodb.calcite.rel.logical.LogicalScanWithRelOp;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public class DingoWindowRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalWindow.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoWindowRule"
        )
        .withRuleFactory(DingoWindowRule::new);

    protected DingoWindowRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final Window winAgg = (Window) rel;
        RelOptTable relOptTable = null;
        if (winAgg.getInput() instanceof RelSubset) {
            RelSubset relSubset = (RelSubset) winAgg.getInput();
            relOptTable = relSubset.getRelList().stream().map(RelNode::getTable).filter(Objects::nonNull).findAny().orElse(null);
        }
        if (relOptTable == null) {
            relOptTable = DingoRelOptTable.getDefault(rel.getCluster());
        }
        final RelTraitSet traitSet =
            winAgg.getTraitSet().replace(DingoConvention.INSTANCE).replace(DingoRelStreaming.of(relOptTable));
        final RelNode child = winAgg.getInput();
        final RelNode convertedChild =
            convert(child,
                child.getTraitSet().replace(DingoConvention.INSTANCE));
        return new DingoWindow(rel.getCluster(), traitSet, winAgg.getHints(), convertedChild,
            winAgg.getConstants(), winAgg.getRowType(), winAgg.groups);
    }
}
