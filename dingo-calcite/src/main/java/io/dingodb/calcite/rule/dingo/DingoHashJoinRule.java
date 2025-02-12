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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        boolean cast = isEquiCast(joinInfo);
        return joinInfo.isEqui() || cast || isNotEqui(joinInfo);
    }

    public static boolean isNotEqui(JoinInfo joinInfo) {
        if (!joinInfo.nonEquiConditions.isEmpty() && joinInfo.rightKeys.size() == joinInfo.leftKeys.size()) {
            RexNode rexNode = joinInfo.nonEquiConditions.get(0);
            if (rexNode instanceof RexCall) {
                RexCall rexCall = (RexCall) rexNode;
                if (rexCall.op.kind == SqlKind.NOT_EQUALS) {
                    return true;
                }
            }
        }
        return true;
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

    public static boolean isEquiCast(JoinInfo joinInfo) {
        if (joinInfo.nonEquiConditions.size() == 1) {
            RexNode rexNode = joinInfo.nonEquiConditions.get(0);
            return isEquiCastOp(rexNode);
        }
        return false;
    }

    public static boolean isEquiCastOp(RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            if (call.op instanceof SqlBinaryOperator) {
                SqlBinaryOperator sqlBinaryOperator = (SqlBinaryOperator) call.op;
                boolean res = call.getOperands().get(0) instanceof RexInputRef
                    && isCastOperator(call.getOperands().get(1));
                boolean res1 = isCastOperator(call.getOperands().get(0))
                    && call.getOperands().get(1) instanceof RexInputRef;
                return sqlBinaryOperator.kind == SqlKind.EQUALS && (res || res1);
            }
        }
        return false;
    }

    private static boolean isCastOperator(RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            return rexCall.op instanceof SqlCastFunction;
        }
        return false;
    }

    public static void splitJoinCondition(
        RexBuilder rexBuilder, int leftFieldCount, RexNode condition, List<Integer> leftKeys, List<Integer> rightKeys
    ) {
        List<Boolean> filterNulls = new ArrayList();
        List<RexNode> nonEquiList = new ArrayList();
        splitJoinCondition(rexBuilder, leftFieldCount, condition, leftKeys, rightKeys, filterNulls, nonEquiList);
    }

    private static void splitJoinCondition(
        RexBuilder rexBuilder, int leftFieldCount, RexNode condition, List<Integer> leftKeys, List<Integer> rightKeys,
        @Nullable List<Boolean> filterNulls, List<RexNode> nonEquiList
    ) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall)condition;
            SqlKind kind = call.getKind();
            if (kind == SqlKind.AND) {
                Iterator var14 = call.getOperands().iterator();

                while(var14.hasNext()) {
                    RexNode operand = (RexNode)var14.next();
                    splitJoinCondition(rexBuilder, leftFieldCount, operand, leftKeys, rightKeys, filterNulls, nonEquiList);
                }

                return;
            }

            if (filterNulls != null) {
                call = RelOptUtil.collapseExpandedIsNotDistinctFromExpr(call, rexBuilder);
                kind = call.getKind();
            }

            if (kind == SqlKind.EQUALS || filterNulls != null && kind == SqlKind.IS_NOT_DISTINCT_FROM) {
                List<RexNode> operands = call.getOperands();
                if (operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexInputRef) {
                    RexInputRef op0 = (RexInputRef)operands.get(0);
                    RexInputRef op1 = (RexInputRef)operands.get(1);
                    RexInputRef leftField;
                    RexInputRef rightField;
                    if (op0.getIndex() < leftFieldCount && op1.getIndex() >= leftFieldCount) {
                        leftField = op0;
                        rightField = op1;
                    } else {
                        if (op1.getIndex() >= leftFieldCount || op0.getIndex() < leftFieldCount) {
                            nonEquiList.add(condition);
                            return;
                        }

                        leftField = op1;
                        rightField = op0;
                    }

                    leftKeys.add(leftField.getIndex());
                    rightKeys.add(rightField.getIndex() - leftFieldCount);
                    if (filterNulls != null) {
                        filterNulls.add(kind == SqlKind.EQUALS);
                    }

                    return;
                } else if (operands.get(0) instanceof RexInputRef && isCastOperator(operands.get(1))) {
                    RexInputRef op0 = (RexInputRef)operands.get(0);
                    RexCall callTmp = (RexCall) operands.get(1);
                    RexInputRef op1 = (RexInputRef) callTmp.getOperands().get(0);
                    RexInputRef leftField;
                    RexInputRef rightField;
                    if (op0.getIndex() < leftFieldCount && op1.getIndex() >= leftFieldCount) {
                        leftField = op0;
                        rightField = op1;
                    } else {
                        if (op1.getIndex() >= leftFieldCount || op0.getIndex() < leftFieldCount) {
                            nonEquiList.add(condition);
                            return;
                        }

                        leftField = op1;
                        rightField = op0;
                    }

                    leftKeys.add(leftField.getIndex());
                    rightKeys.add(rightField.getIndex() - leftFieldCount);
                    if (filterNulls != null) {
                        filterNulls.add(kind == SqlKind.EQUALS);
                    }

                    return;
                } else if (isCastOperator(operands.get(0)) && operands.get(1) instanceof RexInputRef) {
                    RexCall callTmp = (RexCall) operands.get(0);
                    RexInputRef op0 = (RexInputRef) callTmp.getOperands().get(0);
                    RexInputRef op1 = (RexInputRef)operands.get(1);
                    RexInputRef leftField;
                    RexInputRef rightField;
                    if (op0.getIndex() < leftFieldCount && op1.getIndex() >= leftFieldCount) {
                        leftField = op0;
                        rightField = op1;
                    } else {
                        if (op1.getIndex() >= leftFieldCount || op0.getIndex() < leftFieldCount) {
                            nonEquiList.add(condition);
                            return;
                        }

                        leftField = op1;
                        rightField = op0;
                    }

                    leftKeys.add(leftField.getIndex());
                    rightKeys.add(rightField.getIndex() - leftFieldCount);
                    if (filterNulls != null) {
                        filterNulls.add(kind == SqlKind.EQUALS);
                    }

                    return;
                }
            }
        }

        if (!condition.isAlwaysTrue()) {
            nonEquiList.add(condition);
        }

    }
}
