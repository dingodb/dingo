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

import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Optional;

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
//        boolean res = false;
//        // select * from a left join b on a.id=b.id and a.no>6
//        // nonEquiConditions contain a.no > 6
//        if (!joinInfo.nonEquiConditions.isEmpty()) {
//            res = nonEqui(joinInfo.nonEquiConditions);
//        }
        return joinInfo.isEqui();
    }

//    public static boolean nonEqui(List<RexNode> nonEquiList) {
//        for (RexNode condition : nonEquiList) {
//            if (condition instanceof RexCall) {
//                RexCall rexCall = (RexCall) condition;
//                Optional<RexNode> optional
//                    = rexCall.getOperands().stream().filter(rexNode -> rexNode instanceof RexLiteral).findFirst();
//                if (rexCall.getOperands().size() == 2 && optional.isPresent()) {
//                    return true;
//                }
//            }
//        }
//        return false;
//    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        RelTraitSet traits = join.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.ROOT);
        return new DingoHashJoin(
            join.getCluster(),
            traits,
            join.getHints(),
            convert(join.getLeft(), traits),
            convert(join.getRight(), traits),
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType()
        );
    }
}
