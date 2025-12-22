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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;

@Slf4j
@Value.Enclosing
public class DingoFilterReduceExpressionsRule extends RelRule implements SubstitutionRule {

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    public DingoFilterReduceExpressionsRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final List<RexNode> expList =
            Lists.newArrayList(filter.getCondition());
        RexNode newConditionExp;
        boolean reduced;
        final RelMetadataQuery mq = call.getMetadataQuery();
        final RelOptPredicateList predicates =
            mq.getPulledUpPredicates(filter.getInput());
        if (reduceExpressions(filter, expList, predicates, true,
            true, false)) {
            assert expList.size() == 1;
            newConditionExp = expList.get(0);
            reduced = true;
        } else {
            // No reduction, but let's still test the original
            // predicate to see if it was already a constant,
            // in which case we don't need any runtime decision
            // about filtering.
            newConditionExp = filter.getCondition();
            reduced = false;
        }

        // Even if no reduction, let's still test the original
        // predicate to see if it was already a constant,
        // in which case we don't need any runtime decision
        // about filtering.
        if (newConditionExp.isAlwaysTrue()) {
            call.transformTo(
                filter.getInput());
        } else if (newConditionExp instanceof RexLiteral
            || RexUtil.isNullLiteral(newConditionExp, true)) {
            call.transformTo(createEmptyRelOrEquivalent(call, filter));
        //} else if (reduced) {
        //    call.transformTo(call.builder()
        //        .push(filter.getInput())
        //        .filter(newConditionExp).build());
        } else {
            if (newConditionExp instanceof RexCall) {
                boolean reverse = newConditionExp.getKind() == SqlKind.NOT;
                if (reverse) {
                    newConditionExp = ((RexCall) newConditionExp).getOperands().get(0);
                }
                reduceNotNullableFilter(call, filter, newConditionExp, reverse);
            }
            return;
        }

        // New plan is absolutely better than old plan.
        call.getPlanner().prune(filter);
    }

    protected RelNode createEmptyRelOrEquivalent(RelOptRuleCall call, Filter input) {
        return call.builder().push(input).empty().build();
    }

    private void reduceNotNullableFilter(
        RelOptRuleCall call,
        Filter filter,
        RexNode rexNode,
        boolean reverse) {
        // If the expression is a IS [NOT] NULL on a non-nullable
        // column, then we can either remove the filter or replace
        // it with an Empty.
        boolean alwaysTrue;
        switch (rexNode.getKind()) {
            case IS_NULL:
            case IS_UNKNOWN:
                alwaysTrue = false;
                break;
            case IS_NOT_NULL:
                alwaysTrue = true;
                break;
            default:
                return;
        }
        if (reverse) {
            alwaysTrue = !alwaysTrue;
        }
        RexNode operand = ((RexCall) rexNode).getOperands().get(0);
        if (operand instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) operand;
            if (!inputRef.getType().isNullable()) {
                if (alwaysTrue) {
                    call.transformTo(filter.getInput());
                } else {
                    call.transformTo(createEmptyRelOrEquivalent(call, filter));
                }
                // New plan is absolutely better than old plan.
                call.getPlanner().prune(filter);
            }
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoFilterReduceExpressionsRule.Config.builder()
            .operandSupplier(b0 -> b0.operand(LogicalFilter.class).anyInputs())
            .description("ReduceExpressionsRule(DingoFilter)")
            .build();

        @Override default DingoFilterReduceExpressionsRule toRule() {
            return new DingoFilterReduceExpressionsRule(this);
        }
    }

    protected static boolean reduceExpressions(RelNode rel, List<RexNode> expList,
                                               RelOptPredicateList predicates, boolean unknownAsFalse,
                                               boolean matchNullability, boolean treatDynamicCallsAsConstant) {
        final RelOptCluster cluster = rel.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final List<RexNode> originExpList = Lists.newArrayList(expList);
        final RexExecutor executor =
            Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
        final RexSimplify simplify =
            new RexSimplify(rexBuilder, predicates, executor);

        // Simplify predicates in place
        final RexUnknownAs unknownAs = RexUnknownAs.falseIf(unknownAsFalse);
        final boolean reduced = reduceExpressionsInternal(rel, simplify, unknownAs,
            expList, predicates, treatDynamicCallsAsConstant);

        boolean simplified = false;
        for (int i = 0; i < expList.size(); i++) {
            final RexNode expr2 =
                simplify.simplifyPreservingType(expList.get(i), unknownAs,
                    matchNullability);
            if (!expr2.equals(expList.get(i))) {
                expList.set(i, expr2);
                simplified = true;
            }
        }

        if (reduced && simplified) {
            return !originExpList.equals(expList);
        }

        return reduced || simplified;
    }

    protected static class RexReplacer extends RexShuttle {
        private final RexSimplify simplify;
        private final List<RexNode> reducibleExps;
        private final List<RexNode> reducedValues;
        private final List<Boolean> addCasts;

        RexReplacer(
            RexSimplify simplify,
            RexUnknownAs unknownAs,
            List<RexNode> reducibleExps,
            List<RexNode> reducedValues,
            List<Boolean> addCasts) {
            this.simplify = simplify;
            this.reducibleExps = reducibleExps;
            this.reducedValues = reducedValues;
            this.addCasts = addCasts;
        }

        @Override public RexNode visitInputRef(RexInputRef inputRef) {
            RexNode node = visit(inputRef);
            if (node == null) {
                return super.visitInputRef(inputRef);
            }
            return node;
        }

        @Override public RexNode visitCall(RexCall call) {
            RexNode node = visit(call);
            if (node != null) {
                return node;
            }
            node = super.visitCall(call);
            return node;
        }

        private @Nullable RexNode visit(final RexNode call) {
            int i = reducibleExps.indexOf(call);
            if (i == -1) {
                return null;
            }
            RexNode replacement = reducedValues.get(i);
            if (addCasts.get(i)
                && (replacement.getType() != call.getType())) {
                // Handle change from nullable to NOT NULL by claiming
                // that the result is still nullable, even though
                // we know it isn't.
                //
                // Also, we cannot reduce CAST('abc' AS VARCHAR(4)) to 'abc'.
                // If we make 'abc' of type VARCHAR(4), we may later encounter
                // the same expression in a Project's digest where it has
                // type VARCHAR(3), and that's wrong.
                replacement =
                    simplify.rexBuilder.makeAbstractCast(call.getType(), replacement);
            }
            return replacement;
        }
    }

    protected static boolean reduceExpressionsInternal(RelNode rel,
                                                       RexSimplify simplify, RexUnknownAs unknownAs, List<RexNode> expList,
                                                       RelOptPredicateList predicates, boolean treatDynamicCallsAsConstant) {
        // Replace predicates on CASE to CASE on predicates.
        boolean changed = new CaseShuttle().mutate(expList);

        // Find reducible expressions.
        final List<RexNode> constExps = new ArrayList<>();
        List<Boolean> addCasts = new ArrayList<>();
        findReducibleExps(rel.getCluster().getTypeFactory(), expList,
            predicates.constantMap, constExps, addCasts, treatDynamicCallsAsConstant);
        if (constExps.isEmpty()) {
            return changed;
        }

        final List<RexNode> constExps2 = Lists.newArrayList(constExps);
        if (!predicates.constantMap.isEmpty()) {
            final List<Map.Entry<RexNode, RexNode>> pairs =
                Lists.newArrayList(predicates.constantMap.entrySet());
            RexReplacer replacer =
                new RexReplacer(simplify, unknownAs, Pair.left(pairs),
                    Pair.right(pairs), Collections.nCopies(pairs.size(), false));
            replacer.mutate(constExps2);
        }

        // Compute the values they reduce to.
        RexExecutor executor = rel.getCluster().getPlanner().getExecutor();
        if (executor == null) {
            // Cannot reduce expressions: caller has not set an executor in their
            // environment. Caller should execute something like the following before
            // invoking the planner:
            //
            // final RexExecutorImpl executor =
            //   new RexExecutorImpl(Schemas.createDataContext(null));
            // rootRel.getCluster().getPlanner().setExecutor(executor);
            //return changed;
            executor = new RexExecutorImpl(DataContexts.EMPTY);
        }

        final List<RexNode> reducedValues = new ArrayList<>();
        try {
            executor.reduce(simplify.rexBuilder, constExps2, reducedValues);
        } catch (Throwable e) {
            return false;
        }

        // Use RexNode.digest to judge whether each newly generated RexNode
        // is equivalent to the original one.
        if (RexUtil.strings(constExps).equals(RexUtil.strings(reducedValues))) {
            return changed;
        }

        // For Project, we have to be sure to preserve the result
        // types, so always cast regardless of the expression type.
        // For other RelNodes like Filter, in general, this isn't necessary,
        // and the presence of casts could hinder other rules such as sarg
        // analysis, which require bare literals.  But there are special cases,
        // like when the expression is a UDR argument, that need to be
        // handled as special cases.
        if (rel instanceof Project) {
            addCasts = Collections.nCopies(reducedValues.size(), true);
        }

        new RexReplacer(simplify, unknownAs, constExps, reducedValues, addCasts)
            .mutate(expList);
        return true;
    }

    protected static class CaseShuttle extends RexShuttle {
        @Override public RexNode visitCall(RexCall call) {
            for (;;) {
                call = (RexCall) super.visitCall(call);
                final RexCall old = call;
                call = pushPredicateIntoCase(call);
                if (call == old) {
                    return call;
                }
            }
        }
    }

    protected static void findReducibleExps(RelDataTypeFactory typeFactory,
                                            List<RexNode> exps, ImmutableMap<RexNode, RexNode> constants,
                                            List<RexNode> constExps, List<Boolean> addCasts, boolean treatDynamicCallsAsConstant) {
        ReducibleExprLocator gardener =
            new ReducibleExprLocator(typeFactory, constants, constExps,
                addCasts, treatDynamicCallsAsConstant);
        for (RexNode exp : exps) {
            gardener.analyze(exp);
        }
        assert constExps.size() == addCasts.size();
    }

    public static RexCall pushPredicateIntoCase(RexCall call) {
        if (call.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
            return call;
        }
        switch (call.getKind()) {
            case CASE:
            case AND:
            case OR:
                return call; // don't push CASE into CASE!
            case EQUALS: {
                // checks that the EQUALS operands may be split and
                // doesn't push EQUALS into CASE
                List<RexNode> equalsOperands = call.getOperands();
                ImmutableBitSet left = RelOptUtil.InputFinder.bits(equalsOperands.get(0));
                ImmutableBitSet right = RelOptUtil.InputFinder.bits(equalsOperands.get(1));
                if (!left.isEmpty() && !right.isEmpty() && left.intersect(right).isEmpty()) {
                    return call;
                }
                break;
            }
            default:
                break;
        }
        int caseOrdinal = -1;
        final List<RexNode> operands = call.getOperands();
        for (int i = 0; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            if (operand.getKind() == SqlKind.CASE) {
                caseOrdinal = i;
            }
        }
        if (caseOrdinal < 0) {
            return call;
        }
        // Convert
        //   f(CASE WHEN p1 THEN v1 ... END, arg)
        // to
        //   CASE WHEN p1 THEN f(v1, arg) ... END
        final RexCall case_ = (RexCall) operands.get(caseOrdinal);
        final List<RexNode> nodes = new ArrayList<>();
        for (int i = 0; i < case_.getOperands().size(); i++) {
            RexNode node = case_.getOperands().get(i);
            if (!RexUtil.isCasePredicate(case_, i)) {
                node = substitute(call, caseOrdinal, node);
            }
            nodes.add(node);
        }
        return case_.clone(call.getType(), nodes);
    }

    protected static RexNode substitute(RexCall call, int ordinal, RexNode node) {
        final List<RexNode> newOperands = Lists.newArrayList(call.getOperands());
        newOperands.set(ordinal, node);
        return call.clone(call.getType(), newOperands);
    }

    protected static class ReducibleExprLocator extends RexVisitorImpl<Void> {
        /** Whether an expression is constant, and if so, whether it can be
         * reduced to a simpler constant. */
        enum Constancy {
            NON_CONSTANT, REDUCIBLE_CONSTANT, IRREDUCIBLE_CONSTANT
        }

        private final boolean treatDynamicCallsAsConstant;

        private final List<ReducibleExprLocator.Constancy> stack = new ArrayList<>();

        private final ImmutableMap<RexNode, RexNode> constants;

        private final List<RexNode> constExprs;

        private final List<Boolean> addCasts;

        private final Deque<SqlOperator> parentCallTypeStack = new ArrayDeque<>();

        ReducibleExprLocator(RelDataTypeFactory typeFactory,
                             ImmutableMap<RexNode, RexNode> constants, List<RexNode> constExprs,
                             List<Boolean> addCasts, boolean treatDynamicCallsAsConstant) {
            // go deep
            super(true);
            this.constants = constants;
            this.constExprs = constExprs;
            this.addCasts = addCasts;
            this.treatDynamicCallsAsConstant = treatDynamicCallsAsConstant;
        }

        public void analyze(RexNode exp) {
            assert stack.isEmpty();

            exp.accept(this);

            // Deal with top of stack
            assert stack.size() == 1;
            assert parentCallTypeStack.isEmpty();
            ReducibleExprLocator.Constancy rootConstancy = stack.get(0);
            if (rootConstancy == ReducibleExprLocator.Constancy.REDUCIBLE_CONSTANT) {
                // The entire subtree was constant, so add it to the result.
                addResult(exp);
            }
            stack.clear();
        }

        private Void pushVariable() {
            stack.add(ReducibleExprLocator.Constancy.NON_CONSTANT);
            return null;
        }

        private void addResult(RexNode exp) {
            // Cast of literal can't be reduced, so skip those (otherwise we'd
            // go into an infinite loop as we add them back).
            if (exp.getKind() == SqlKind.CAST) {
                RexCall cast = (RexCall) exp;
                RexNode operand = cast.getOperands().get(0);
                if (operand instanceof RexLiteral) {
                    return;
                }
            }
            constExprs.add(exp);

            // In the case where the expression corresponds to a UDR argument,
            // we need to preserve casts.  Note that this only applies to
            // the topmost argument, not expressions nested within the UDR
            // call.
            //
            // REVIEW zfong 6/13/08 - Are there other expressions where we
            // also need to preserve casts?
            SqlOperator op = parentCallTypeStack.peek();
            if (op == null) {
                addCasts.add(false);
            } else {
                addCasts.add(isUdf(op));
            }
        }

        private static Boolean isUdf(@SuppressWarnings("unused") SqlOperator operator) {
            // return operator instanceof UserDefinedRoutine
            return false;
        }

        @Override public Void visitInputRef(RexInputRef inputRef) {
            final RexNode constant = constants.get(inputRef);
            if (constant != null) {
                if (constant instanceof RexCall || constant instanceof RexDynamicParam) {
                    constant.accept(this);
                } else {
                    stack.add(ReducibleExprLocator.Constancy.REDUCIBLE_CONSTANT);
                }
                return null;
            }
            return pushVariable();
        }

        @Override public Void visitLiteral(RexLiteral literal) {
            stack.add(ReducibleExprLocator.Constancy.IRREDUCIBLE_CONSTANT);
            return null;
        }

        @Override public Void visitOver(RexOver over) {
            // assume non-constant (running SUM(1) looks constant but isn't)
            analyzeCall(over, ReducibleExprLocator.Constancy.NON_CONSTANT);
            return null;
        }

        @Override public Void visitCorrelVariable(RexCorrelVariable variable) {
            return pushVariable();
        }

        @Override public Void visitCall(RexCall call) {
            // assume REDUCIBLE_CONSTANT until proven otherwise
            analyzeCall(call, ReducibleExprLocator.Constancy.REDUCIBLE_CONSTANT);
            return null;
        }

        @Override public Void visitSubQuery(RexSubQuery subQuery) {
            analyzeCall(subQuery, ReducibleExprLocator.Constancy.REDUCIBLE_CONSTANT);
            return null;
        }

        private void analyzeCall(RexCall call, ReducibleExprLocator.Constancy callConstancy) {
            parentCallTypeStack.push(call.getOperator());

            // visit operands, pushing their states onto stack
            super.visitCall(call);

            // look for NON_CONSTANT operands
            int operandCount = call.getOperands().size();
            List<ReducibleExprLocator.Constancy> operandStack = Util.last(stack, operandCount);
            for (ReducibleExprLocator.Constancy operandConstancy : operandStack) {
                if (operandConstancy == ReducibleExprLocator.Constancy.NON_CONSTANT) {
                    callConstancy = ReducibleExprLocator.Constancy.NON_CONSTANT;
                    break;
                }
            }

            // Even if all operands are constant, the call itself may
            // be non-deterministic.
            if (!call.getOperator().isDeterministic()) {
                callConstancy = ReducibleExprLocator.Constancy.NON_CONSTANT;
            } else if (!treatDynamicCallsAsConstant
                && call.getOperator().isDynamicFunction()) {
                // In some circumstances, we should avoid caching the plan if we have dynamic functions.
                // If desired, treat this situation the same as a non-deterministic function.
                callConstancy = ReducibleExprLocator.Constancy.NON_CONSTANT;
            }

            // Row operator itself can't be reduced to a literal, but if
            // the operands are constants, we still want to reduce those
            if ((callConstancy == ReducibleExprLocator.Constancy.REDUCIBLE_CONSTANT)
                && (call.getOperator() instanceof SqlRowOperator)) {
                callConstancy = ReducibleExprLocator.Constancy.NON_CONSTANT;
            }

            if (callConstancy == ReducibleExprLocator.Constancy.NON_CONSTANT) {
                // any REDUCIBLE_CONSTANT children are now known to be maximal
                // reducible subtrees, so they can be added to the result
                // list
                for (int iOperand = 0; iOperand < operandCount; ++iOperand) {
                    ReducibleExprLocator.Constancy constancy = operandStack.get(iOperand);
                    if (constancy == ReducibleExprLocator.Constancy.REDUCIBLE_CONSTANT) {
                        addResult(call.getOperands().get(iOperand));
                    }
                }
            }

            // pop operands off of the stack
            operandStack.clear();

            // pop this parent call operator off the stack
            parentCallTypeStack.pop();

            // push constancy result for this call onto stack
            stack.add(callConstancy);
        }

        @Override public Void visitDynamicParam(RexDynamicParam dynamicParam) {
            return pushVariable();
        }

        @Override public Void visitRangeRef(RexRangeRef rangeRef) {
            return pushVariable();
        }

        @Override public Void visitFieldAccess(RexFieldAccess fieldAccess) {
            return pushVariable();
        }
    }
}
