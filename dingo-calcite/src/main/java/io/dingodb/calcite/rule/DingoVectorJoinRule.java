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

import io.dingodb.calcite.rule.dingo.DingoHashJoinRule;
import io.dingodb.calcite.rel.LogicalDingoVector;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.type.DingoSqlTypeFactory;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.immutables.value.Value;

import static io.dingodb.calcite.rule.DingoVectorIndexRule.getDingoGetVectorByDistance;

@Value.Enclosing
public class DingoVectorJoinRule extends RelRule<DingoVectorJoinRule.Config>  {
    /**
     * Creates a RelRule.
     *
     * @param config
     */
    public DingoVectorJoinRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin logicalJoin = call.rel(0);

        LogicalJoin newLogicalJoin = (LogicalJoin) logicalJoin.copy(logicalJoin.getTraitSet(), logicalJoin.getInputs());

        RelSubset subset = (RelSubset) newLogicalJoin.getLeft();
        if (subset.getRelList().size() == 1) {
            RelNode relNode = subset.getBestOrOriginal();
            if (relNode instanceof LogicalDingoVector) {
                LogicalDingoVector vector = (LogicalDingoVector) relNode;
                RelTraitSet traits = vector.getTraitSet()
                    .replace(DingoConvention.INSTANCE)
                    .replace(DingoRelStreaming.of(vector.getTable()));
                LogicalDingoVector replaceVector = (LogicalDingoVector) vector.copy(traits, vector.getInputs(),
                    vector.getCall(), vector.getElementType(), vector.getRowType(), vector.getColumnMappings());
                RexBuilder rexBuilder = new RexBuilder(DingoSqlTypeFactory.INSTANCE);
                RexLiteral rexLiteral = rexBuilder.makeLiteral("1");
                RexLiteral rexLiteral1 = rexBuilder.makeLiteral("1");
                RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexLiteral, rexLiteral1);
                RelNode left = getDingoGetVectorByDistance(condition, replaceVector, true);
                newLogicalJoin.replaceInput(0, left);
                call.transformTo(newLogicalJoin);
            }
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        DingoVectorJoinRule.Config DEFAULT = ImmutableDingoVectorJoinRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalJoin.class).predicate(rel -> {
                    if (!DingoHashJoinRule.match(rel)) {
                        return false;
                    }
                    if (rel.getHints().size() == 0) {
                        return false;
                    } else {
                        if (!"vector_pre".equalsIgnoreCase(rel.getHints().get(0).hintName)) {
                            return false;
                        }
                    }
                    if (rel.getLeft() instanceof RelSubset) {
                        RelSubset subset = (RelSubset) rel.getLeft();
                        if (subset.getRelList().size() == 1) {
                            RelNode relNode = subset.getBestOrOriginal();
                            if (relNode instanceof LogicalDingoVector) {
                                return true;
                            }
                        }
                    }
                    return false;
                }).anyInputs()
            )
            .description("DingoVectorJoinRule")
            .build();

        @Override
        default DingoVectorJoinRule toRule() {
            return new DingoVectorJoinRule(this);
        }
    }
}
