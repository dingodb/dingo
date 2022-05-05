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
import io.dingodb.calcite.rel.DingoUnion;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class DingoUnionRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(
            LogicalUnion.class,
            Convention.NONE,
            DingoConventions.ROOT,
            "DingoUnionRule"
        )
        .withRuleFactory(DingoUnionRule::new);

    private DingoUnionRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(@Nonnull RelNode rel) {
        Union union = (Union) rel;
        return new DingoUnion(
            union.getCluster(),
            union.getTraitSet().replace(DingoConventions.ROOT),
            union.getInputs().stream()
                .map(n -> convert(n, DingoConventions.ROOT))
                .collect(Collectors.toList()),
            union.all
        );
    }
}
