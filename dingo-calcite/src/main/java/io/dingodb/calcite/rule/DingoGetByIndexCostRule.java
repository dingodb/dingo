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
import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.rel.logical.LogicalDocumentScanFilter;
import io.dingodb.calcite.rel.logical.LogicalIndexFullScan;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.utils.IndexValueMapSet;
import io.dingodb.calcite.utils.IndexValueMapSetVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Pair;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Value.Enclosing
public class DingoGetByIndexCostRule extends RelRule<DingoGetByIndexCostRule.Config> {
    protected DingoGetByIndexCostRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalDingoTableScan scan = call.rel(0);
        RexNode rexNode = RexUtil.toDnf(scan.getCluster().getRexBuilder(), scan.getFilter());
        rexNode = DingoGetByIndexRule.eliminateSpecialCast(rexNode, scan.getCluster().getRexBuilder());
        IndexValueMapSetVisitor visitor = new IndexValueMapSetVisitor(scan.getCluster().getRexBuilder());
        IndexValueMapSet<Integer, RexNode> indexValueMapSet = rexNode.accept(visitor);
        final Table table = Objects.requireNonNull(scan.getTable().unwrap(DingoTable.class)).getTable();
        List<Integer> keyIndices = Arrays.stream(table.keyMapping().getMappings()).boxed().collect(Collectors.toList());
        Set<Map<Integer, RexNode>> keyMapSet = DingoGetByIndexRule.filterIndices(indexValueMapSet, keyIndices, scan.getSelection());
        if (keyMapSet != null) {
            RelTraitSet traits = scan.getTraitSet()
                .replace(DingoConvention.INSTANCE)
                .replace(DingoRelStreaming.of(scan.getTable()));
            DingoGetByKeys dingoGetByKeys = new DingoGetByKeys(
                scan.getCluster(),
                traits,
                scan.getHints(),
                scan.getTable(),
                scan.getFilter(),
                scan.getSelection(),
                keyMapSet
            );
            call.transformTo(dingoGetByKeys);
            return;
        }

        boolean disableIndex = !scan.getHints().isEmpty()
            && "disable_index".equalsIgnoreCase(scan.getHints().get(0).hintName);
        if (disableIndex) {
            return;
        }
        Map<CommonId, Table> indexTdMap = DingoGetByIndexRule.getScalaIndices(scan.getTable());

        LogUtils.debug(log, "Definition of table = {}", table);
        if (indexTdMap.isEmpty()) {
            return;
        }

        // index a, index b :  a = v or b = v   ==> matched index a, index b
        // index a, index b :  a = v and b = v  ==> matched first matched index a
        // indexSetMap key = matched indexId , value = index point value
        Map<CommonId, Set> indexSetMap = filterScalarIndices(indexValueMapSet, indexTdMap, scan.getSelection(), table);
        if (indexSetMap == null) {
            return;
        }
        RelTraitSet traits = scan.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.of(scan.getTable()));
        if (indexSetMap.size() > 1) {
            for (Map.Entry<CommonId, Set> indexSet : indexSetMap.entrySet()) {
                Map<CommonId, Set> itemIndex = new HashMap<>();
                itemIndex.put(indexSet.getKey(), indexSet.getValue());
                call.transformTo(new DingoGetByIndex(
                    scan.getCluster(),
                    traits,
                    scan.getHints(),
                    scan.getTable(),
                    scan.getFilter(),
                    scan.getRealSelection(),
                    false,
                    itemIndex,
                    indexTdMap,
                    scan.isForDml()
                ));
            }
        } else {
            call.transformTo(new DingoGetByIndex(
                scan.getCluster(),
                traits,
                scan.getHints(),
                scan.getTable(),
                scan.getFilter(),
                scan.getRealSelection(),
                false,
                indexSetMap,
                indexTdMap,
                scan.isForDml()
            ));
        }

    }

    public static @Nullable Map<CommonId, Set> filterScalarIndices(
        @NonNull IndexValueMapSet<Integer, RexNode> mapSet,
        @NonNull Map<CommonId, Table> indexTdMap,
        TupleMapping selection,
        Table td
    ) {
        Set<Map<Integer, RexNode>> set = mapSet.getSet();
        if (set != null) {
            List<Integer> indices;
            Map<CommonId, Set> indexMap = new HashMap<>();
            boolean matchIndex;
            // example: a column index =2, b column index = 3
            // set : a=v or a=v1 =>
            // Set item count is 2:
            //   Map1<entry(key = 2, value = v)>,
            //   Map2<entry(key = 2, value = v1)>
            // set : a=v and b=v1 =>
            // Set item count is 1:
            // Map1<entry(key = 2, value = v), entry(key = 3, value = v1)>
            for (Map<Integer, RexNode> map : set) {
                matchIndex = false;
                for (Map.Entry<CommonId, Table> index : indexTdMap.entrySet()) {
                    if (!index.getValue().visible) {
                        continue;
                    }
                    indices = td.getColumnIndices2(index.getValue().columns);
                    Map<Integer, RexNode> newMap = new HashMap<>(indices.size());
                    for (int k : map.keySet()) {
                        if (selection != null && k >= selection.size()) {
                            return null;
                        }
                        int originIndex = (selection == null ? k : selection.get(k));
                        if (indices.contains(originIndex)) {
                            newMap.put(indices.indexOf(originIndex), map.get(k));
                        }
                    }
                    // Leftmost matching principle
                    // index a columns(b, c, d)  -> where b = v  ==> matched index a
                    if (newMap.containsKey(0)) {
                        Set<Map<Integer, RexNode>> newSet
                            = indexMap.computeIfAbsent(index.getKey(), e -> new HashSet());
                        newSet.add(newMap);
                        matchIndex = true;
                    }
                }
                if (!matchIndex) {
                    return null;
                }
            }
            if (indexMap.isEmpty()) {
                return null;
            }
            return indexMap;
        }
        return null;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {

        DingoGetByIndexCostRule.Config DEFAULT = ImmutableDingoGetByIndexCostRule.Config.builder()
            .description("DingoGetByIndexCostRule")
            .operandSupplier(b0 ->
                b0.operand(LogicalDingoTableScan.class)
                    .predicate(rel -> rel.getFilter() != null
                        && !(rel instanceof LogicalIndexFullScan || rel instanceof LogicalDocumentScanFilter)).noInputs()
            )
            .build();

        @Override
        default DingoGetByIndexCostRule toRule() {
            return new DingoGetByIndexCostRule(this);
        }

    }
}
