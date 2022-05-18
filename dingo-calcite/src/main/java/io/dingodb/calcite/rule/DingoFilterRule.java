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
import io.dingodb.calcite.rel.DingoFilter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;

public class DingoFilterRule extends ConverterRule {
    public static final Config DISTRIBUTED = Config.INSTANCE
        .withConversion(
            LogicalFilter.class,
            Convention.NONE,
            DingoConventions.DISTRIBUTED,
            "DingoFilterRule.DISTRIBUTED"
        )
        .withRuleFactory(DingoFilterRule::new);
    public static final Config ROOT = Config.INSTANCE
        .withConversion(
            LogicalFilter.class,
            Convention.NONE,
            DingoConventions.ROOT,
            "DingoFilterRule.ROOT"
        )
        .withRuleFactory(DingoFilterRule::new);

    protected DingoFilterRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        Filter filter = (Filter) rel;
        Convention convention = this.getOutConvention();
        return new DingoFilter(
            filter.getCluster(),
            filter.getTraitSet().replace(convention),
            convert(filter.getInput(), convention),
            filter.getCondition()
        );
    }
}
