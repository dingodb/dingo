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
import io.dingodb.calcite.rel.DingoProject;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public class DingoProjectRule extends ConverterRule {
    public static final Config DISTRIBUTED = Config.INSTANCE
        .withConversion(
            LogicalProject.class,
            Convention.NONE,
            DingoConventions.DISTRIBUTED,
            "DingoProjectRule.DISTRIBUTED"
        )
        .withRuleFactory(DingoProjectRule::new);
    public static final Config ROOT = Config.INSTANCE
        .withConversion(
            LogicalProject.class,
            Convention.NONE,
            DingoConventions.ROOT,
            "DingoProjectRule.ROOT"
        )
        .withRuleFactory(DingoProjectRule::new);

    protected DingoProjectRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject project = (LogicalProject) rel;
        Convention convention = this.getOutConvention();
        return new DingoProject(
            project.getCluster(),
            project.getTraitSet().replace(convention),
            project.getHints(),
            convert(project.getInput(), convention),
            project.getProjects(),
            project.getRowType()
        );
    }
}
