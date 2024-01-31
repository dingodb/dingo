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

package io.dingodb.calcite.rule.logical;

import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.rel.logical.LogicalScanWithRelOp;
import io.dingodb.calcite.rule.DingoTableScanRule;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.rules.SubstitutionRule;

import java.util.List;

public class LogicalScanWithRelOpRule extends ConverterRule implements SubstitutionRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalDingoTableScan.class,
            (LogicalDingoTableScan scan) ->
                scan.getFilter() == null
                    && scan.getRealSelection() == null
                    && scan.getAggCalls() == null
                    && scan.getGroupSet() == null
                    && scan.getGroupSets() == null,
            Convention.NONE,
            Convention.NONE,
            "LogicalScanWithRelOpRule"
        )
        .withRuleFactory(LogicalScanWithRelOpRule::new);

    protected LogicalScanWithRelOpRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalDingoTableScan scan = (LogicalDingoTableScan) rel;
        List<String> fullNameList = scan.getTable().getQualifiedName();
        if (DingoTableScanRule.metaSchemaList.contains(fullNameList.get(1))) {
            return null;
        }
        if (scan.getSelection().size() < scan.getRowType().getFieldCount()) {
            return null;
        }
        return new LogicalScanWithRelOp(
            scan.getCluster(),
            scan.getTraitSet(),
            scan.getHints(),
            scan.getTable(),
            scan.getRowType(),
            null,
            scan.isPushDown()
        );
    }
}
