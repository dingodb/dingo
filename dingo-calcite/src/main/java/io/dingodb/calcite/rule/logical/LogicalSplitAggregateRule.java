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

package io.dingodb.calcite.rule.logical;

import com.google.common.collect.ImmutableSet;
import io.dingodb.calcite.rel.logical.LogicalReduceAggregate;
import io.dingodb.calcite.rel.logical.LogicalRelOp;
import io.dingodb.exec.expr.DingoCompileContext;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

import java.util.List;
import java.util.Set;

import static io.dingodb.common.util.Utils.sole;

@Value.Enclosing
public class LogicalSplitAggregateRule extends RelRule<LogicalSplitAggregateRule.Config> implements SubstitutionRule {
    // TODO: GROUPING is not supported, maybe it is useful.
    private static final Set<SqlKind> supportedAggregations = ImmutableSet.of(
        SqlKind.COUNT,
        SqlKind.SUM,
        SqlKind.SUM0,
        SqlKind.MAX,
        SqlKind.MIN,
        SqlKind.SINGLE_VALUE
    );

    protected LogicalSplitAggregateRule(Config config) {
        super(config);
    }

    public static boolean match(@NonNull LogicalAggregate rel) {
        return rel.getAggCallList().stream().noneMatch(agg -> {
            SqlKind kind = agg.getAggregation().getKind();
            // AVG must be transformed to SUM/COUNT before.
            if (!supportedAggregations.contains(kind)) {
                return true;
            }
            // After apply `CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES`, the sql: `select count(distinct a) from t`
            // will be transformed to two rel nodes:
            // 1. aggregate with distinct(AggregateCall List is empty)
            // 2. aggregate with count(AggregateCall List contains COUNT, SUM, AVG...)
            // So, In this case, the origin aggregate and distinct should be ignored.
            return agg.isDistinct() && (kind == SqlKind.COUNT || kind == SqlKind.SUM);
        });
    }

    static @NonNull Expr getAgg(@NonNull AggregateCall agg) {
        SqlKind kind = agg.getAggregation().getKind();
        List<Integer> args = agg.getArgList();
        if (args.isEmpty() && kind == SqlKind.COUNT) {
            return Exprs.op(Exprs.COUNT_ALL_AGG);
        }
        int index = sole(args);
        Expr var = DingoCompileContext.createTupleVar(index);
        switch (kind) {
            case COUNT:
                return Exprs.op(Exprs.COUNT_AGG, var);
            case SUM:
                return Exprs.op(Exprs.SUM_AGG, var);
            case SUM0:
                return Exprs.op(Exprs.SUM0_AGG, var);
            case MAX:
                return Exprs.op(Exprs.MAX_AGG, var);
            case MIN:
                return Exprs.op(Exprs.MIN_AGG, var);
            case SINGLE_VALUE:
                return Exprs.op(Exprs.SINGLE_VALUE_AGG, var);
            default:
                break;
        }
        throw new UnsupportedOperationException("Unsupported aggregation function \"" + kind + "\".");
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        final LogicalAggregate aggregate = call.rel(0);
        final List<AggregateCall> aggCallList = aggregate.getAggCallList();
        // In `Aggregate`, type checks were done but with `assert`, which is not effective in production.
        for (AggregateCall aggCall : aggCallList) {
            SqlKind aggKind = aggCall.getAggregation().getKind();
            if (aggKind == SqlKind.SUM || aggKind == SqlKind.SUM0) {
                if (aggCall.type.getFamily() != SqlTypeFamily.NUMERIC) {
                    throw new IllegalArgumentException(
                        "Aggregation function \"" + aggKind + "\" requires numerical input but \""
                            + aggCall.type + "\" was given."
                    );
                }
            }
        }
        int[] groupIndices = aggregate.getGroupSet().asList().stream()
            .mapToInt(Integer::intValue)
            .toArray();
        Expr[] exprs = aggregate.getAggCallList().stream()
            .map(LogicalSplitAggregateRule::getAgg)
            .toArray(Expr[]::new);
        RelOp relOp;
        if (groupIndices.length == 0) {
            relOp = RelOpBuilder.builder().agg(exprs).build();
        } else {
            relOp = RelOpBuilder.builder().agg(groupIndices, exprs).build();
        }
        LogicalRelOp rel = new LogicalRelOp(
            aggregate.getCluster(),
            aggregate.getTraitSet(),
            aggregate.getHints(),
            aggregate.getInput(),
            aggregate.getRowType(),
            relOp
        );
        call.transformTo(
            new LogicalReduceAggregate(
                aggregate.getCluster(),
                aggregate.getTraitSet(),
                aggregate.getHints(),
                rel,
                relOp,
                aggregate.getInput().getRowType()
            )
        );
        call.getPlanner().prune(aggregate);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableLogicalSplitAggregateRule.Config.builder()
            .operandSupplier(b0 ->
                b0.operand(LogicalAggregate.class).predicate(LogicalSplitAggregateRule::match).anyInputs()
            )
            .description("LogicalSplitAggregateRule")
            .build();

        @Override
        default LogicalSplitAggregateRule toRule() {
            return new LogicalSplitAggregateRule(this);
        }
    }
}
