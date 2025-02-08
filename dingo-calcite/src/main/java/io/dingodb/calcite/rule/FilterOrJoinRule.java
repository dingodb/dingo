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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.dingodb.calcite.plan.RelUtil;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Value.Enclosing
public class FilterOrJoinRule extends RelRule<RelRule.Config> {
    protected FilterOrJoinRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Join join = call.rel(1);
        perform(call, filter, join);
    }

    protected void perform(RelOptRuleCall call, @Nullable Filter filter, Join join) {
        List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
        List<RexNode> origJoinFilters = ImmutableList.copyOf(joinFilters);
        if (filter != null || !joinFilters.isEmpty()) {
            List<RexNode> aboveFilters = filter != null ? getConjunctions(filter) : new ArrayList();
            ImmutableList<RexNode> origAboveFilters = ImmutableList.copyOf((Collection)aboveFilters);
            JoinRelType joinType = join.getJoinType();
            if (!origAboveFilters.isEmpty() && join.getJoinType() != JoinRelType.INNER) {
                joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
            }

            List<RexNode> leftFilters = new ArrayList();
            List<RexNode> rightFilters = new ArrayList();
            boolean filterPushed = RelUtil.classifyFiltersOr(join, aboveFilters, joinType.canPushIntoFromAbove(), joinType.canPushLeftFromAbove(), joinType.canPushRightFromAbove(), joinFilters, leftFilters, rightFilters);

            //this.validateJoinFilters(aboveFilters, joinFilters, join, joinType);
            if (leftFilters.isEmpty() && rightFilters.isEmpty() && joinFilters.size() == origJoinFilters.size() && aboveFilters.size() == origAboveFilters.size() && Sets.newHashSet(joinFilters).equals(Sets.newHashSet(origJoinFilters))) {
                filterPushed = false;
            }

            if (joinType != JoinRelType.FULL) {
                joinFilters = this.inferJoinEqualConditions(joinFilters, join);
            }

            if (RelUtil.classifyFiltersOr(join, joinFilters, false, joinType.canPushLeftFromWithin(), joinType.canPushRightFromWithin(), joinFilters, leftFilters, rightFilters)) {
                filterPushed = true;
            }

            if ((filterPushed || joinType != join.getJoinType()) && (!joinFilters.isEmpty() || !leftFilters.isEmpty() || !rightFilters.isEmpty())) {
                RexBuilder rexBuilder = join.getCluster().getRexBuilder();
                RelBuilder relBuilder = call.builder();
                RelNode leftRel = relBuilder.push(join.getLeft()).filter(leftFilters).build();
                RelNode rightRel = relBuilder.push(join.getRight()).filter(rightFilters).build();
                List<RelDataType> fieldTypes = new ArrayList<>();
                fieldTypes.addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()));
                fieldTypes.addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType()));
                RexNode joinFilter = RexUtil.composeConjunction(rexBuilder, RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes));
                if (!joinFilter.isAlwaysTrue() || !leftFilters.isEmpty() || !rightFilters.isEmpty() || joinType != join.getJoinType()) {
                    RelNode newJoinRel = join.copy(join.getTraitSet(), joinFilter, leftRel, rightRel, joinType, join.isSemiJoinDone());
                    call.getPlanner().onCopy(join, newJoinRel);
                    if (!leftFilters.isEmpty() && filter != null) {
                        call.getPlanner().onCopy(filter, leftRel);
                    }

                    if (!rightFilters.isEmpty() && filter != null) {
                        call.getPlanner().onCopy(filter, rightRel);
                    }

                    relBuilder.push(newJoinRel);
                    relBuilder.convert(join.getRowType(), false);
                    //relBuilder.filter(RexUtil.fixUp(rexBuilder, aboveFilters, RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));
                    call.transformTo(relBuilder.build());
                }
            }
        }
    }

    protected void validateJoinFilters(List<RexNode> aboveFilters, List<RexNode> joinFilters, Join join, JoinRelType joinType) {
        Iterator<RexNode> filterIter = joinFilters.iterator();

        while (filterIter.hasNext()) {
            RexNode exp = filterIter.next();
            if (false) {
                aboveFilters.add(exp);
                filterIter.remove();
            }
        }
    }

    protected List<RexNode> inferJoinEqualConditions(List<RexNode> rexNodes, Join join) {
        List<RexNode> result = new ArrayList(rexNodes.size());
        List<Set<RexInputRef>> equalSets = splitEqualSets(rexNodes, result);
        boolean needOptimize = false;
        Iterator var6 = equalSets.iterator();

        while(var6.hasNext()) {
            Set<RexInputRef> set = (Set)var6.next();
            if (set.size() > 2) {
                needOptimize = true;
                break;
            }
        }

        if (!needOptimize) {
            return rexNodes;
        } else {
            result.addAll(constructConditionFromEqualSets(join, equalSets));
            return result;
        }
    }

    private static List<Set<RexInputRef>> splitEqualSets(List<RexNode> rexNodes, List<RexNode> leftNodes) {
        List<Set<RexInputRef>> equalSets = new ArrayList<>();
        Iterator<RexNode> var3 = rexNodes.iterator();

        while(true) {
            while(true) {
                while (var3.hasNext()) {
                    RexNode rexNode = var3.next();
                    if (rexNode.isA(SqlKind.EQUALS)) {
                        RexNode op1 = ((RexCall)rexNode).getOperands().get(0);
                        RexNode op2 = ((RexCall)rexNode).getOperands().get(1);
                        if (op1 instanceof RexInputRef && op2 instanceof RexInputRef) {
                            RexInputRef in1 = (RexInputRef)op1;
                            RexInputRef in2 = (RexInputRef)op2;
                            Set<RexInputRef> set = null;
                            Iterator var10 = equalSets.iterator();

                            label46: {
                                Set s;
                                do {
                                    if (!var10.hasNext()) {
                                        break label46;
                                    }

                                    s = (Set)var10.next();
                                } while(!s.contains(in1) && !s.contains(in2));

                                set = s;
                            }

                            if (set == null) {
                                set = new LinkedHashSet();
                                equalSets.add(set);
                            }

                            set.add(in1);
                            set.add(in2);
                        } else {
                            leftNodes.add(rexNode);
                        }
                    } else {
                        leftNodes.add(rexNode);
                    }
                }

                return equalSets;
            }
        }
    }

    private static List<RexNode> constructConditionFromEqualSets(Join join, List<Set<RexInputRef>> equalSets) {
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        List<RexNode> result = new ArrayList();
        int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        Iterator var5 = equalSets.iterator();

        while(var5.hasNext()) {
            Set<RexInputRef> set = (Set)var5.next();
            List<RexInputRef> leftSet = new ArrayList();
            List<RexInputRef> rightSet = new ArrayList();
            Iterator var9 = set.iterator();

            while(var9.hasNext()) {
                RexInputRef ref = (RexInputRef)var9.next();
                if (ref.getIndex() < leftFieldCount) {
                    leftSet.add(ref);
                } else {
                    rightSet.add(ref);
                }
            }

            int i;
            if (leftSet.size() > 1) {
                for(i = 1; i < leftSet.size(); ++i) {
                    result.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftSet.get(0), leftSet.get(i)));
                }
            }

            if (rightSet.size() > 1) {
                for(i = 1; i < rightSet.size(); ++i) {
                    result.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rightSet.get(0), rightSet.get(i)));
                }
            }

            if (leftSet.size() > 0 && rightSet.size() > 0) {
                result.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftSet.get(0), rightSet.get(0)));
            }
        }

        return result;
    }

    private static List<RexNode> getConjunctions(Filter filter) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(filter.getCondition());
        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

        for (int i = 0; i < conjunctions.size(); ++i) {
            RexNode node = conjunctions.get(i);
            if (node instanceof RexCall) {
                conjunctions.set(i, RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall)node, rexBuilder));
            }
        }

        return conjunctions;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        FilterOrJoinRule.Config DEFAULT = ImmutableFilterOrJoinRule.Config.builder()
            .description("FilterOrJoinRule")
            .operandSupplier(b0 ->
                b0.operand(Filter.class).oneInput(b1 ->
                    b1.operand(Join.class).anyInputs()
                )
            )
            .build();

        @Override
        default FilterOrJoinRule toRule() {
            return new FilterOrJoinRule(this);
        }

    }
}
