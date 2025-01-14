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

import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.rel.logical.LogicalDocumentScanFilter;
import io.dingodb.calcite.rel.logical.LogicalIndexFullScan;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.utils.GlobalVariablesUtil;
import io.dingodb.calcite.utils.IndexValueMapSet;
import io.dingodb.calcite.utils.IndexValueMapSetVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.type.SqlTypeName;
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
            rel -> rel.getFilter() != null && !(rel instanceof LogicalIndexFullScan || rel instanceof LogicalDocumentScanFilter),
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
                    columnList = index.getValue().getColumns();
                    indices = columnList.stream().map(td.getColumns()::indexOf).collect(Collectors.toList());
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
                        break;
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

    @Override
    public @Nullable RelNode convert(@NonNull RelNode rel) {
        LogicalDingoTableScan scan = (LogicalDingoTableScan) rel;
        RexNode rexNode = RexUtil.toDnf(scan.getCluster().getRexBuilder(), scan.getFilter());
        rexNode = eliminateSpecialCast(rexNode, scan.getCluster().getRexBuilder());
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
        //if (!scan.isForDml()) {
        //    return null;
        //}

        // get all index definition
        // find match first index to use;
        // new DingoGetIndex
        boolean disableIndex = !scan.getHints().isEmpty()
            && "disable_index".equalsIgnoreCase(scan.getHints().get(0).hintName);
        if (disableIndex) {
            return null;
        }
        Map<CommonId, Table> indexTdMap = getScalaIndices(scan.getTable());

        LogUtils.debug(log, "Definition of table = {}", table);
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
            return null;
            //return new DingoGetByIndexMerge(
            //    scan.getCluster(),
            //    traits,
            //    scan.getHints(),
            //    scan.getTable(),
            //    scan.getFilter(),
            //    scan.getRealSelection(),
            //    false,
            //    indexSetMap,
            //    indexTdMap,
            //    table.keyMapping(),
            //    true
            //);
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

    public static Map<CommonId, Table> getDocumentIndices(LogicalDingoTableScan scan, RelOptTable relOptTable) {
        Map<CommonId, Table> indexTdMap = new HashMap<>();
        if (!GlobalVariablesUtil.getEnableDocumentScanFilter((DingoParserContext) scan.getCluster().getPlanner().getContext())) {
            return indexTdMap;
        }
        DingoTable dingoTable = relOptTable.unwrap(DingoTable.class);
        assert dingoTable != null;
        List<IndexTable> indexes = dingoTable.getTable().getIndexes();
        for (IndexTable index : indexes) {
            if (index.getSchemaState() != SchemaState.SCHEMA_PUBLIC) {
                continue;
            }
            if (index.getProperties() == null) {
                continue;
            }
            if (index.indexType == IndexType.DOCUMENT) {
                indexTdMap.put(index.getTableId(), index);
            }
        }
        return indexTdMap;
    }

    public static Map<CommonId, Table> getScalaIndices(RelOptTable relOptTable) {
        Map<CommonId, Table> indexTdMap = new HashMap<>();
        DingoTable dingoTable = relOptTable.unwrap(DingoTable.class);
        assert dingoTable != null;
        List<IndexTable> indexes = dingoTable.getTable().getIndexes();
        for (IndexTable index : indexes) {
            if (index.getSchemaState() != SchemaState.SCHEMA_PUBLIC) {
                continue;
            }
            if (index.getProperties() == null) {
                continue;
            }
            if (!index.indexType.isVector && index.indexType != IndexType.DOCUMENT) {
                indexTdMap.put(index.getTableId(), index);
            }
        }
        return indexTdMap;
    }

    /**
    *  example1: create table t(id varchar(3), age int, primary key(id)).
    *  select * from t where id='xxxxxxx'
    *  if condition val length greater than column precision then general
     *  expr rexCall like cast inputRef(0) to varchar(7)
    *  this expr can not batch get.
    *  example2: create table t1(id int,address varchar(30), price float,primary key(id), index ax(price))
    *  select price from t1 where price=3.3; If the condition uses an index, but the index field is of float type,
    *  the index cannot be used
    */
    public static RexNode eliminateSpecialCast(RexNode relNode, RexBuilder rexBuilder) {
        if (!(relNode instanceof RexCall)) {
            return relNode;
        }
        RexCall call = (RexCall) relNode;
        if (call.getOperands().size() == 2 && (call.getOperands().get(0) instanceof RexCall)
            && call.getOperands().get(1) instanceof RexLiteral) {
            RexCall operandCall = (RexCall) call.getOperands().get(0);
            boolean isCastFun = operandCall.op instanceof SqlCastFunction;
            boolean checkOperands = operandCall.getOperands().size() == 1
                && operandCall.getOperands().get(0) instanceof RexInputRef;
            boolean operand0PreCheck = isCastFun && checkOperands;
            if (!operand0PreCheck) {
                return relNode;
            }

            RexLiteral rexLiteral = (RexLiteral) call.getOperands().get(1);
            if (operandCall.type.getSqlTypeName() == SqlTypeName.VARCHAR
                && rexLiteral.getTypeName() == SqlTypeName.CHAR) {
                return rexBuilder.makeCall(call.op, operandCall.getOperands().get(0), rexLiteral);
            } else if (operandCall.type.getSqlTypeName() == SqlTypeName.DOUBLE
                && rexLiteral.getTypeName() == SqlTypeName.DECIMAL) {
                return rexBuilder.makeCall(call.op, operandCall.getOperands().get(0), rexLiteral);
            }
        }
        return relNode;

    }

}
