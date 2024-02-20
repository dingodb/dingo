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

import io.dingodb.calcite.rel.DingoExportData;
import io.dingodb.calcite.rel.LogicalExportData;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DingoExportDataRule extends ConverterRule {

    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalExportData.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoExportDataRule"
        )
        .withRuleFactory(DingoExportDataRule::new);

    protected DingoExportDataRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        LogicalExportData logicalExportData = (LogicalExportData) rel;
        RelTraitSet traits = logicalExportData.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.ROOT);
        return new DingoExportData(
            logicalExportData.getCluster(),
            traits,
            convert(logicalExportData.getInput(), traits),
            logicalExportData.getOutfile(),
            logicalExportData.getTerminated(),
            logicalExportData.getStatementId(),
            logicalExportData.getEnclosed(),
            logicalExportData.getLineTerminated(),
            logicalExportData.getEscaped(),
            logicalExportData.getCharset(),
            logicalExportData.getLineStarting(),
            logicalExportData.getTimeZone()
        );
    }
}
