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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.SubstitutionRule;

public class LogicalRelOpFromProjectRule extends ConverterRule implements SubstitutionRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalProject.class,
            Convention.NONE,
            Convention.NONE,
            "LogicalRelOpFromProjectRule"
        )
        .withRuleFactory(LogicalRelOpFromProjectRule::new);

    protected LogicalRelOpFromProjectRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject project = (LogicalProject) rel;
        try {
            Expr[] exprs = project.getProjects().stream()
                .map(RexConverter::convert)
                .toArray(Expr[]::new);
            RelOp relOp = RelOpBuilder.builder()
                .project(exprs)
                .build();
            return new LogicalRelOp(
                project.getCluster(),
                project.getTraitSet(),
                project.getHints(),
                project.getInput(),
                project.getRowType(),
                relOp
            );
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }
}
