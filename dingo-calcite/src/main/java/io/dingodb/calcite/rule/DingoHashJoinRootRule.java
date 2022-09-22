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

package io.dingodb.calcite.rule;

import io.dingodb.calcite.DingoConventions;
import io.dingodb.calcite.rel.DingoHashJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DingoHashJoinRootRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalJoin.class,
            DingoHashJoinRule::match,
            Convention.NONE,
            DingoConventions.ROOT,
            "DingoHashJoinRootRule.ROOT"
        )
        .withRuleFactory(DingoHashJoinRootRule::new);

    protected DingoHashJoinRootRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        JoinInfo joinInfo = join.analyzeCondition();
        if (!joinInfo.isEqui()) {
            return null;
        }
        return new DingoHashJoin(
            join.getCluster(),
            join.getTraitSet().replace(DingoConventions.ROOT),
            join.getHints(),
            convert(join.getLeft(), DingoConventions.ROOT),
            convert(join.getRight(), DingoConventions.ROOT),
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType()
        );
    }
}
