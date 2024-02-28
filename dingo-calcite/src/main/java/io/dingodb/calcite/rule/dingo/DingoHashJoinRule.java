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

import io.dingodb.calcite.rel.dingo.DingoHashJoin;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DingoHashJoinRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalJoin.class,
            DingoHashJoinRule::match,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoHashJoinRule"
        )
        .withRuleFactory(DingoHashJoinRule::new);

    protected DingoHashJoinRule(Config config) {
        super(config);
    }

    /**
     * Non-equiv join condition is extracted by {@link org.apache.calcite.rel.rules.JoinExtractFilterRule}, so check
     * with this method to process only equiv join.
     */
    public static boolean match(@NonNull LogicalJoin rel) {
        JoinInfo joinInfo = rel.analyzeCondition();
        return joinInfo.isEqui();
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        JoinInfo joinInfo = join.analyzeCondition();
        RelTraitSet traits, leftTraits, rightTraits;
        if (!joinInfo.leftKeys.isEmpty() && !joinInfo.rightKeys.isEmpty()) { // Can be partitioned.
            traits = join.getTraitSet()
                .replace(DingoConvention.INSTANCE)
                .replace(DingoRelStreaming.of(joinInfo.leftKeys));
            leftTraits = traits.replace(DingoRelStreaming.of(joinInfo.leftKeys));
            rightTraits = traits.replace(DingoRelStreaming.of(joinInfo.rightKeys));
        } else {
            traits = join.getTraitSet()
                .replace(DingoConvention.INSTANCE)
                .replace(DingoRelStreaming.ROOT);
            leftTraits = traits;
            rightTraits = traits;
        }
        return new DingoHashJoin(
            join.getCluster(),
            traits,
            join.getHints(),
            convert(join.getLeft(), leftTraits),
            convert(join.getRight(), rightTraits),
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType()
        );
    }
}
