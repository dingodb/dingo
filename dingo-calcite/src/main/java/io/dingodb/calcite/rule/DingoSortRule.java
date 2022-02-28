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
import io.dingodb.calcite.rel.DingoSort;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;

import javax.annotation.Nonnull;

public class DingoSortRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(
            LogicalSort.class,
            Convention.NONE,
            DingoConventions.ROOT,
            "DingoSortRule"
        )
        .withRuleFactory(DingoSortRule::new);

    protected DingoSortRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(@Nonnull RelNode rel) {
        LogicalSort sort = (LogicalSort) rel;
        RelTraitSet traitSet = sort.getTraitSet().replace(DingoConventions.ROOT);
        return new DingoSort(
            sort.getCluster(),
            traitSet,
            convert(sort.getInput(), traitSet),
            sort.getCollation(),
            sort.offset,
            sort.fetch
        );
    }
}
