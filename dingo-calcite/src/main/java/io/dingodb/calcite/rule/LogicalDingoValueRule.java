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

import io.dingodb.calcite.rel.LogicalDingoValues;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.RexLiteralUtils;
import io.dingodb.common.type.DingoType;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.stream.Collectors;

public class LogicalDingoValueRule extends ConverterRule implements SubstitutionRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalValues.class,
            Convention.NONE,
            Convention.NONE,
            "DingoLogicalValuesRule"
        )
        .withRuleFactory(LogicalDingoValueRule::new);

    protected LogicalDingoValueRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(@NonNull RelNode rel) {
        RelDataType rowType = rel.getRowType();
        DingoType type = DefinitionMapper.mapToDingoType(rowType);
        return new LogicalDingoValues(
            rel.getCluster(),
            rel.getTraitSet(),
            rowType,
            ((LogicalValues) rel).getTuples().stream()
                .map(t -> RexLiteralUtils.convertFromRexLiteralList(t, type))
                .collect(Collectors.toList())
        );
    }
}
