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
import io.dingodb.calcite.rel.DingoGetByIndexMerge;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.utils.IndexValueMapSet;
import io.dingodb.calcite.utils.IndexValueMapSetVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

    public static @Nullable Set<Map<Integer, RexNode>> filterIndices(
        @NonNull IndexValueMapSet<Integer, RexNode> mapSet,
        @NonNull List<@NonNull Integer> indices,
        TupleMapping selection
    ) {
        Set<Map<Integer, RexNode>> set = mapSet.getSet();
        if (set != null) {
            Set<Map<Integer, RexNode>> newSet = new HashSet<>();
            for (Map<Integer, RexNode> map : set) {
                Map<Integer, RexNode> newMap = new HashMap<>(indices.size());
                for (int k : map.keySet()) {
                    int originIndex = (selection == null ? k : selection.get(k));
                    if (indices.contains(originIndex)) {
                        newMap.put(originIndex, map.get(k));
                    }
                }
                if (!newMap.keySet().containsAll(indices)) {
                    return null;
                }
                newSet.add(newMap);
            }
            return newSet;
        }
        return null;
    }

    public static @Nullable Map<CommonId, Set> filterScalarIndices(
        @NonNull IndexValueMapSet<Integer, RexNode> mapSet,
        @NonNull Map<CommonId, Table> indexTdMap,
        TupleMapping selection,
        Table td
    ) {
        Set<Map<Integer, RexNode>> set = mapSet.getSet();
        if (set != null) {
            List<Column> columnList;
            List<Integer> indices;
            Map<CommonId, Set> indexMap = new HashMap<>();
            boolean matchIndex;
            // example: a column index =2, b column index = 3
            // set1 : a=v or a=v1 => Map1<entry(key = 2, value = v)>, Map2<entry(key = 2, value = v1)>
            // set2 : a=v and b=v1 => Map1<entry(key = 2, value = v), entry(key = 2, value = v1)>
            for (Map<Integer, RexNode> map : set) {
                matchIndex = false;
                for (Map.Entry<CommonId, Table> index : indexTdMap.entrySet()) {
                    columnList = index.getValue().getColumns();
                    indices = columnList.stream().map(td.getColumns()::indexOf).collect(Collectors.toList());
                    Map<Integer, RexNode> newMap = new HashMap<>(indices.size());
                    for (int k : map.keySet()) {
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
                        break;
                    }
                }
                if (!matchIndex) {
                    return null;
                }
            }
            if (indexMap.size() == 0) {
                return null;
            }
            return indexMap;
        }
        return null;
    }

    @Override
    public @Nullable RelNode convert(@NonNull RelNode rel) {
        LogicalDingoTableScan scan = (LogicalDingoTableScan) rel;
        RexNode rexNode = RexUtil.toDnf(scan.getCluster().getRexBuilder(), scan.getFilter());
        IndexValueMapSetVisitor visitor = new IndexValueMapSetVisitor(rel.getCluster().getRexBuilder());
        IndexValueMapSet<Integer, RexNode> indexValueMapSet = rexNode.accept(visitor);
        final Table table = Objects.requireNonNull(scan.getTable().unwrap(DingoTable.class)).getTable();
        List<Integer> keyIndices = Arrays.stream(table.keyMapping().getMappings()).boxed().collect(Collectors.toList());
        Set<Map<Integer, RexNode>> keyMapSet = filterIndices(indexValueMapSet, keyIndices, scan.getSelection());
        if (keyMapSet != null) {
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
                keyMapSet
            );
        }
        //The update index column will not use indexes for now, and will be changed in the next version
        //if (scan.isForDml()) {
        //    return null;
        //}

        // get all index definition
        // find match first index to use; todo replace
        // new DingoGetIndex
        Map<CommonId, Table> indexTdMap = getScalaIndices(scan.getTable());

        if (log.isDebugEnabled()) {
            log.debug("Definition of table = {}", table);
        }
        if (indexTdMap.isEmpty()) {
            return null;
        }

        // index a, index b :  a = v or b = v   ==> matched index a, index b
        // index a, index b :  a = v and b = v  ==> matched first matched index a
        // indexSetMap key = matched indexId , value = index point value
        Map<CommonId, Set> indexSetMap = filterScalarIndices(indexValueMapSet, indexTdMap, scan.getSelection(), table);
        if (indexSetMap == null) {
            return null;
        }
        RelTraitSet traits = scan.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.of(scan.getTable()));
        if (indexSetMap.size() > 1) {
            return new DingoGetByIndexMerge(
                scan.getCluster(),
                traits,
                scan.getHints(),
                scan.getTable(),
                scan.getFilter(),
                scan.getRealSelection(),
                false,
                indexSetMap,
                indexTdMap,
                table.keyMapping(),
                true
            );
        } else {
            return new DingoGetByIndex(
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
            );
        }
    }

    public static Map<CommonId, Table> getScalaIndices(RelOptTable relOptTable) {
        Map<CommonId, Table> indexTdMap = new HashMap<>();
        DingoTable dingoTable = relOptTable.unwrap(DingoTable.class);
        assert dingoTable != null;
        List<IndexTable> indexes = dingoTable.getTable().getIndexes();
        for (IndexTable index : indexes) {
            if (index.getProperties() == null) {
                continue;
            }
            if (!index.indexType.isVector) {
                indexTdMap.put(index.getTableId(), index);
            }
        }
        return indexTdMap;
    }

}
