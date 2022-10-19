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
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoExchange;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DingoExchangeRootRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            RelNode.class,
            DingoConventions.DISTRIBUTED,
            DingoConventions.ROOT,
            "DingoExchangeRootRule.ROOT"
        )
        .withRuleFactory(DingoExchangeRootRule::new);

    protected DingoExchangeRootRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(@NonNull RelNode rel) {
        RelOptCluster cluster = rel.getCluster();
        return new DingoCoalesce(
            cluster,
            rel.getTraitSet().replace(DingoConventions.ROOT),
            new DingoExchange(
                cluster,
                // The changing of trait is crucial, or the rule would be recursively applied to it.
                rel.getTraitSet().replace(DingoConventions.PARTITIONED),
                rel,
                true
            )
        );
    }
}
