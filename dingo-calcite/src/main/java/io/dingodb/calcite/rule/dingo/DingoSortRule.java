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

import io.dingodb.calcite.rel.dingo.DingoSort;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;

public class DingoSortRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalSort.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoSortRule"
        )
        .withRuleFactory(DingoSortRule::new);

    protected DingoSortRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalSort sort = (LogicalSort) rel;
        RelTraitSet traits = sort.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.ROOT);
        // The input need not be sorted.
        RelTraitSet inputTraits = traits.replace(RelCollations.EMPTY);
        return new DingoSort(
            sort.getCluster(),
            traits,
            convert(sort.getInput(), inputTraits),
            sort.getCollation(),
            sort.offset,
            sort.fetch
        );
    }
}
