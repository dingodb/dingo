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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoInfoSchemaScan;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoTransientTableScan;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.impl.ListTransientTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class DingoTableScanForSpoolRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalTableScan.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoTableScanForSpoolRule"
        )
        .withRuleFactory(DingoTableScanForSpoolRule::new);

    public DingoTableScanForSpoolRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode relNode) {
        LogicalTableScan scan = (LogicalTableScan) relNode;
        RelTraitSet traits = scan.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.of(scan.getTable()));
        List<String> fullNameList = scan.getTable().getQualifiedName();
        if (fullNameList.size() >= 2 && DingoTableScanRule.metaSchemaSet.contains(fullNameList.get(1))) {
            DingoTable dingoTable = scan.getTable().unwrap(DingoTable.class);
            if (dingoTable != null && "SYSTEM VIEW".equals(dingoTable.getTable().getTableType())) {
                return new DingoInfoSchemaScan(
                    scan.getCluster(),
                    traits,
                    scan.getHints(),
                    scan.getTable(),
                    null,
                   null
                );
            }
        }
        ListTransientTable transientTable = scan.getTable().unwrap(ListTransientTable.class);
        if (transientTable != null) {
            return new DingoTransientTableScan(scan.getCluster(), traits, scan.getHints(),
                scan.getTable(), null, null, transientTable);
        }
        return new DingoTableScan(
            scan.getCluster(),
            traits,
            scan.getHints(),
            scan.getTable(),
            null,
            null, // selection
            null,
            null,
            null,
            false,
            false
        );
    }
}
