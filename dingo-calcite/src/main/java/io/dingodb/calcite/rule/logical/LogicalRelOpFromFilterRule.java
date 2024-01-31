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

import io.dingodb.calcite.rel.logical.LogicalRelOp;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.runtime.expr.Expr;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.SubstitutionRule;

public class LogicalRelOpFromFilterRule extends ConverterRule implements SubstitutionRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalFilter.class,
            Convention.NONE,
            Convention.NONE,
            "LogicalRelOpFromFilterRule"
        )
        .withRuleFactory(LogicalRelOpFromFilterRule::new);

    protected LogicalRelOpFromFilterRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalFilter filter = (LogicalFilter) rel;
        try {
            Expr expr = RexConverter.convert(filter.getCondition());
            RelOp relOp = RelOpBuilder.builder()
                .filter(expr)
                .build();
            return new LogicalRelOp(
                filter.getCluster(),
                filter.getTraitSet(),
                filter.getHints(),
                filter.getInput(),
                filter.getRowType(),
                relOp
            );
        } catch (RexConverter.UnsupportedRexNode e) {
            return null;
        }
    }
}
