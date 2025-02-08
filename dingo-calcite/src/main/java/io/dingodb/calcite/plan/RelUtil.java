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

package io.dingodb.calcite.plan;

import io.dingodb.calcite.type.DingoSqlTypeFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class RelUtil {

    private RelUtil() {
    }

    public static boolean isJoinFilterOr(List<RexNode> filters, boolean pushInfo, int leftFields, int rightFields) {
        if (filters.size() != 1) {
            return false;
        }
        RexNode rexNode = filters.get(0);
        RexCall rexCall = (RexCall) rexNode;
        boolean or = rexCall.op.kind == SqlKind.OR;
        if (or) {
            List<RexNode> operands = rexCall.getOperands();
            List<List<RexNode>> allJoinFilters = new ArrayList<>();
            // find each operand join filter
            for (RexNode subRexNode : operands) {
                List<RexNode> subJoinFilters = findJoinFilter(subRexNode, leftFields, rightFields);
                if (subJoinFilters.isEmpty()) {
                    return false;
                }
                allJoinFilters.add(subJoinFilters);
            }
            return findCommon(allJoinFilters);
        }
        return false;
    }

    public static boolean findCommon(List<List<RexNode>> allJoinFilters) {
        List<RexNode> firstFilters = allJoinFilters.get(0);
        RexNode rexNode = firstFilters.get(0);
        return allJoinFilters.stream().allMatch(filters -> filters.stream()
            .anyMatch(rexNode1 -> matchCommonRexNode(rexNode, rexNode1)));
    }

    private static boolean matchCommonRexNode(RexNode rexNode1, RexNode rexNode2) {
        if (rexNode1 instanceof RexCall && rexNode2 instanceof RexCall) {
            RexCall call1 = (RexCall) rexNode1;
            RexCall call2 = (RexCall) rexNode2;
            if (call1.op.kind == SqlKind.EQUALS && call2.op.kind == SqlKind.EQUALS) {
                return true;
            }
        }
        return false;
    }

    public static List<RexNode> findJoinFilter(RexNode rexNode, int leftFields, int rightFields) {
        List<RexNode> filters = getConjunctions(rexNode);
        int totalFields = leftFields + rightFields;
        ImmutableBitSet leftBitmap = ImmutableBitSet.range(0, leftFields);
        ImmutableBitSet rightBitmap = ImmutableBitSet.range(leftFields, totalFields);
        Iterator<RexNode> var19 = filters.iterator();
        List<RexNode> joinFilters = new ArrayList<>();
        while (var19.hasNext()) {
            RexNode filter = var19.next();
            RelOptUtil.InputFinder inputFinder = RelOptUtil.InputFinder.analyze(filter);
            ImmutableBitSet inputBits = inputFinder.build();
            if (!leftBitmap.contains(inputBits) && !rightBitmap.contains(inputBits)) {
                joinFilters.add(filter);
            }
        }
        return joinFilters;
    }

    private static List<RexNode> getConjunctions(RexNode condition) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);
        RexBuilder rexBuilder = new RexBuilder(DingoSqlTypeFactory.INSTANCE);

        for (int i = 0; i < conjunctions.size(); ++i) {
            RexNode node = conjunctions.get(i);
            if (node instanceof RexCall) {
                conjunctions.set(i, RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall)node, rexBuilder));
            }
        }

        return conjunctions;
    }

    public static boolean classifyFiltersOr(
        RelNode joinRel, List<RexNode> filters, boolean pushInto, boolean pushLeft, boolean pushRight,
        List<RexNode> joinFilters, List<RexNode> leftFilters, List<RexNode> rightFilters
    ) {
        List<RelDataTypeField> leftFields = joinRel.getInputs().get(0).getRowType().getFieldList();
        int nFieldsLeft = leftFields.size();
        List<RelDataTypeField> rightFields = joinRel.getInputs().get(1).getRowType().getFieldList();
        int nFieldsRight = rightFields.size();
        boolean isJoinOr = isJoinFilterOr(filters, pushInto, nFieldsLeft, nFieldsRight);
        if (!isJoinOr) {
            return false;
        }
        RexCall call = (RexCall) filters.get(0);
        List<RexNode> subFilters = call.getOperands();
        List<RexCall> leftCallList = new ArrayList<>();
        List<RexCall> rightCallList = new ArrayList<>();
        for (RexNode subFilter : subFilters) {
            List<RexNode> subLeftFilters = new ArrayList<>();
            List<RexNode> subRightFilters = new ArrayList<>();
            RexCall subCall = (RexCall) subFilter;
            List<RexNode> paramCall = new ArrayList<>();
            subCall.getOperands().forEach(paramCall::add);
            classifyFilters(joinRel, paramCall, pushInto, pushLeft, pushRight,
                joinFilters, subLeftFilters, subRightFilters);
            // make and
            RexBuilder rexBuilder = new RexBuilder(DingoSqlTypeFactory.INSTANCE);
            RexCall leftSubCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.AND, subLeftFilters);
            RexCall rightSubCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.AND, subRightFilters);
            // append to left
            leftCallList.add(leftSubCall);
            rightCallList.add(rightSubCall);
        }
        RexBuilder rexBuilder = new RexBuilder(DingoSqlTypeFactory.INSTANCE);
        leftFilters.add(rexBuilder.makeCall(SqlStdOperatorTable.OR, leftCallList));
        rightFilters.add(rexBuilder.makeCall(SqlStdOperatorTable.OR, rightCallList));
        return true;
    }

    public static boolean classifyFilters(
        RelNode joinRel, List<RexNode> filters, boolean pushInto, boolean pushLeft, boolean pushRight,
        List<RexNode> joinFilters, List<RexNode> leftFilters, List<RexNode> rightFilters
    ) {
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        List<RelDataTypeField> joinFields = joinRel.getRowType().getFieldList();
        boolean nSysFields = false;
        List<RelDataTypeField> leftFields = joinRel.getInputs().get(0).getRowType().getFieldList();
        int nFieldsLeft = leftFields.size();
        List<RelDataTypeField> rightFields = joinRel.getInputs().get(1).getRowType().getFieldList();
        int nFieldsRight = rightFields.size();
        int nTotalFields = nFieldsLeft + nFieldsRight;
        ImmutableBitSet leftBitmap = ImmutableBitSet.range(0, 0 + nFieldsLeft);
        ImmutableBitSet rightBitmap = ImmutableBitSet.range(0 + nFieldsLeft, nTotalFields);
        List<RexNode> filtersToRemove = new ArrayList();
        Iterator var19 = filters.iterator();

        while (true) {
            while (var19.hasNext()) {
                RexNode filter = (RexNode)var19.next();
                RelOptUtil.InputFinder inputFinder = RelOptUtil.InputFinder.analyze(filter);
                ImmutableBitSet inputBits = inputFinder.build();
                RexNode shiftedFilter;
                if (pushLeft && leftBitmap.contains(inputBits)) {
                    if (!filter.isAlwaysTrue()) {
                        shiftedFilter = shiftFilter(0, 0 + nFieldsLeft, 0, rexBuilder, joinFields, nTotalFields, leftFields, filter);
                        leftFilters.add(shiftedFilter);
                    }

                    filtersToRemove.add(filter);
                } else if (pushRight && rightBitmap.contains(inputBits)) {
                    if (!filter.isAlwaysTrue()) {
                        shiftedFilter = shiftFilter(0 + nFieldsLeft, nTotalFields, -(0 + nFieldsLeft), rexBuilder, joinFields, nTotalFields, rightFields, filter);
                        rightFilters.add(shiftedFilter);
                    }

                    filtersToRemove.add(filter);
                } else if (pushInto) {
                    if (!joinFilters.contains(filter) && joinFilters.isEmpty()) {
                        joinFilters.add(filter);
                    }

                    filtersToRemove.add(filter);
                }
            }

            if (!filtersToRemove.isEmpty()) {
                filters.removeAll(filtersToRemove);
            }

            return !filtersToRemove.isEmpty();
        }
    }

    private static RexNode shiftFilter(
        int start, int end, int offset, RexBuilder rexBuilder, List<RelDataTypeField> joinFields,
        int nTotalFields, List<RelDataTypeField> rightFields, RexNode filter
    ) {
        int[] adjustments = new int[nTotalFields];

        for(int i = start; i < end; ++i) {
            adjustments[i] = offset;
        }

        return (RexNode)filter.accept(new RelOptUtil.RexInputConverter(rexBuilder, joinFields, rightFields, adjustments));
    }
}
