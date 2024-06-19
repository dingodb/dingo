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

import io.dingodb.calcite.rel.dingo.IndexFullScan;
import io.dingodb.calcite.rel.logical.LogicalIndexFullScan;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DingoIndexFullScanRule extends ConverterRule {

    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalIndexFullScan.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoIndexFullScanRule"
        )
        .withRuleFactory(DingoIndexFullScanRule::new);

    protected DingoIndexFullScanRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode relNode) {
        LogicalIndexFullScan scan = (LogicalIndexFullScan) relNode;
        RelTraitSet traits = scan.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.of(scan.getTable()));
        return new IndexFullScan(relNode.getCluster(),
                                 traits,
                                 scan.getHints(),
                                 scan.getTable(),
                                 scan.getFilter(),
                                 scan.getSelection(),
                                 scan.getIndexTable(),
                                 scan.getIndexId(),
                                 scan.getSelectionIndexList(),
                                 scan.isPushDown(),
                                 scan.isLookup(),
                                 scan.getKeepSerialOrder()
                                 );
    }
}
