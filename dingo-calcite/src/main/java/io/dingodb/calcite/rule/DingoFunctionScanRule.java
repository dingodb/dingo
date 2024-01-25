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

import io.dingodb.calcite.rel.DingoFunctionScan;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.rel.LogicalDingoVector;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

@Slf4j
@Value.Enclosing
public class DingoFunctionScanRule extends ConverterRule {
    public static final ConverterRule.Config DEFAULT = ConverterRule.Config.INSTANCE
        .withConversion(
            TableFunctionScan.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoFunctionScanRule"
        )
        .withRuleFactory(DingoFunctionScanRule::new);

    public DingoFunctionScanRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        RelTraitSet traits = rel.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.of(rel.getTable()));

        if (rel instanceof LogicalDingoVector && !(rel instanceof DingoVector)) {
            LogicalDingoVector vector = (LogicalDingoVector) rel;
            return new DingoVector(
                vector.getCluster(),
                traits,
                vector.getCall(),
                vector.getTable(),
                vector.getOperands(),
                vector.getIndexTableId(),
                vector.getIndexTable(),
                vector.getSelection(),
                vector.getFilter(),
                vector.hints
            );
        } else if (rel instanceof DingoFunctionScan) {
            DingoFunctionScan scan = (DingoFunctionScan) rel;
            return new DingoFunctionScan(
                scan.getCluster(),
                traits,
                scan.getCall(),
                scan.getTable(),
                scan.getOperands()
            );
        }

        return null;
    }
}
