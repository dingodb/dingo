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

import io.dingodb.calcite.rel.DingoInfoSchemaScan;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import java.util.Collections;
import java.util.List;

public class DingoTableScanRule extends ConverterRule {
    public static final List<String> metaSchemaList = Collections.singletonList("INFORMATION_SCHEMA");

    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalDingoTableScan.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoTableScanRule"
        )
        .withRuleFactory(DingoTableScanRule::new);

    protected DingoTableScanRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalDingoTableScan scan = (LogicalDingoTableScan) rel;
        RelTraitSet traits = scan.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.of(scan.getTable()));
        List<String> fullNameList = scan.getTable().getQualifiedName();
        if (metaSchemaList.contains(fullNameList.get(1))) {
            return new DingoInfoSchemaScan(
                scan.getCluster(),
                traits,
                scan.getHints(),
                scan.getTable(),
                scan.getFilter(),
                scan.getRealSelection()
            );
        } else {
            return new DingoTableScan(
                scan.getCluster(),
                traits,
                scan.getHints(),
                scan.getTable(),
                scan.getFilter(),
                scan.getRealSelection(),
                scan.getAggCalls(),
                scan.getGroupSet(),
                scan.getGroupSets(),
                scan.isPushDown(),
                scan.isForDml()
            );
        }
    }
}
