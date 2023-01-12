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

import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.utils.IndexValueMapSet;
import io.dingodb.calcite.utils.IndexValueMapSetVisitor;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class DingoGetByIndexRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalDingoTableScan.class,
            rel -> rel.getFilter() != null,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoGetByKeysRule"
        )
        .withRuleFactory(DingoGetByIndexRule::new);

    public DingoGetByIndexRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(@NonNull RelNode rel) {
        LogicalDingoTableScan scan = (LogicalDingoTableScan) rel;
        RexNode rexNode = RexUtil.toDnf(scan.getCluster().getRexBuilder(), scan.getFilter());
        TableDefinition td = TableUtils.getTableDefinition(scan.getTable());
        IndexValueMapSetVisitor visitor = new IndexValueMapSetVisitor(td, rel.getCluster().getRexBuilder());
        IndexValueMapSet<Integer, RexLiteral> indexValueMapSet = rexNode.accept(visitor);
        List<Integer> keyIndices = td.getKeyColumnIndices();
        if (indexValueMapSet.satisfyIndices(keyIndices)) {
            RelTraitSet traits = scan.getTraitSet()
                .replace(DingoConvention.INSTANCE)
                .replace(DingoRelStreaming.of(scan.getTable()));
            return new DingoGetByKeys(
                scan.getCluster(),
                traits,
                scan.getHints(),
                scan.getTable(),
                scan.getFilter(),
                scan.getSelection(),
                indexValueMapSet.filterIndices(keyIndices)
            );
        }
        Map<String, Index> indexes = td.getIndexes();
        if (log.isDebugEnabled()) {
            log.debug("Definition of table = {}", td);
        }
        for (Map.Entry<String, Index> entry : indexes.entrySet()) {
            Index index = entry.getValue();
            List<Integer> indices = td.getColumnIndices(Arrays.asList(index.getColumns()));
            if (indexValueMapSet.satisfyIndices(indices)) {
                RelTraitSet traits = scan.getTraitSet()
                    .replace(DingoConvention.INSTANCE)
                    .replace(DingoRelStreaming.ROOT);
                return new DingoGetByIndex(
                    scan.getCluster(),
                    traits,
                    scan.getHints(),
                    scan.getTable(),
                    scan.getFilter(),
                    scan.getSelection(),
                    entry.getKey(),
                    indexValueMapSet.filterIndices(indices)
                );
            }
        }
        return null;
    }
}
