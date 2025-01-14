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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.rel.logical.LogicalDocumentScanFilter;
import io.dingodb.calcite.rel.logical.LogicalIndexFullScan;
import io.dingodb.calcite.rel.logical.LogicalIndexRangeScan;
import io.dingodb.calcite.traits.DingoRelCollationImpl;
import io.dingodb.calcite.utils.DocumentScanFilterOb;
import io.dingodb.calcite.utils.DocumentScanFilterVisitor;
import io.dingodb.calcite.utils.GlobalVariablesUtil;
import io.dingodb.calcite.utils.IndexRangeMapSet;
import io.dingodb.calcite.utils.IndexRangeVisitor;
import io.dingodb.calcite.visitor.RexConverter;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.expr.runtime.expr.IndexOpExpr;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rule.DingoGetByIndexRule.getDocumentIndices;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.getScalaIndices;
import static io.dingodb.calcite.rule.DingoIndexCollationRule.getIndexByExpr;
import static io.dingodb.common.util.Utils.isNeedLookUp;

@Value.Enclosing
public class DingoIndexScanMatchRule extends RelRule<DingoIndexScanMatchRule.Config> implements SubstitutionRule {

    public DingoIndexScanMatchRule(Config config) {
        super(config);
    }

    private static void matchProjectSortOrder(DingoIndexScanMatchRule rule, RelOptRuleCall call) {
        LogicalSort logicalSort = call.rel(0);

        if (RuleUtils.validateDisableIndex(logicalSort.getHints())) {
            return;
        }

        List<RelFieldCollation> relFieldCollationList = logicalSort.getCollation().getFieldCollations();
        if (relFieldCollationList.size() != 1) {
            return;
        }

        RelFieldCollation relFieldCollation = relFieldCollationList.get(0);
        int collationIndex = relFieldCollation.getFieldIndex();
        int keepSerialOrder = RuleUtils.getSerialOrder(relFieldCollation);
        if (RuleUtils.preventRemoveOrder(keepSerialOrder)) {
            return;
        }
        LogicalProject logicalProject = call.rel(1);
        List<RexNode> projects = logicalProject.getProjects();
        if (!(projects.size() > collationIndex && projects.get(collationIndex) instanceof RexInputRef)) {
            return;
        }
        int projectIndex = ((RexInputRef) projects.get(collationIndex)).getIndex();
        LogicalDingoTableScan scan = call.rel(2);
        int colIndex = scan.getSelection().get(projectIndex);
        if (colIndex < 0) {
            return;
        }
        DingoTable dingoTable = scan.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        Column orderCol;
        if (colIndex < dingoTable.getTable().columns.size()) {
            orderCol = dingoTable.getTable().columns.get(colIndex);
            if (RuleUtils.matchRemoveSort(orderCol.primaryKeyIndex, dingoTable.getTable().partitionStrategy)) {
                scan.setKeepSerialOrder(keepSerialOrder);
                call.transformTo(logicalSort.copy(logicalSort.getTraitSet(), logicalProject,
                    new DingoRelCollationImpl(ImmutableList.of(), true, keepSerialOrder)));
                return;
            }
        } else {
            return;
        }
        if (RuleUtils.matchTablePrimary(logicalSort)) {
            return;
        }

        int[] ixs = scan.getSelection().getMappings();
        List<Integer> ixList = new ArrayList<>();
        for (int i : ixs) {
            ixList.add(i);
        }

        Result result = getResult(scan, ixList, scan.getTable());
        if (!(scan.getFilter() == null && result.matchIndex)) {
            return;
        }

        if (validateNotRemoveSort(result.matchIndexTable, orderCol)) {
            return;
        }

        RelCollation relCollation = RelCollationImpl.of(new ArrayList<>());

        List<Integer> indexSelectionList = result.matchIndexTable.getColumns().stream()
            .map(dingoTable.getTable().columns::indexOf).collect(Collectors.toList());
        TupleMapping tupleMapping = TupleMapping.of(
            indexSelectionList
        );
        boolean needLookup = isNeedLookUp(scan.getSelection(), tupleMapping, dingoTable.getTable().columns.size());
        LogicalIndexFullScan indexFullScan = new LogicalIndexFullScan(
            scan.getCluster(),
            scan.getTraitSet(),
            scan.getHints(),
            scan.getTable(),
            scan.getFilter(),
            scan.getSelection(),
            result.matchIndexTable,
            result.indexId,
            result.selectionIxList,
            scan.isPushDown(),
            needLookup,
            keepSerialOrder
        );
        LogicalProject project = logicalProject.copy(logicalProject.getTraitSet(), indexFullScan,
            logicalProject.getProjects(), logicalProject.getRowType());
        LogicalSort newSort = (LogicalSort) logicalSort.copy(logicalSort.getTraitSet(), project, relCollation);
        call.transformTo(newSort);
    }

    private static boolean validateNotRemoveSort(Table result, Column orderCol) {
        int matchSortIx = result.getColumns().indexOf(orderCol);
        if (matchSortIx < 0) {
            return true;
        }
        Column matchOrderIxCol = result.getColumns().get(matchSortIx);
        return !RuleUtils.matchRemoveSort(matchOrderIxCol.primaryKeyIndex, result.partitionStrategy);
    }

    // example1: ix is index
    // a. select ix from table order by ix;
    // b. select * from table order by ix limit 3
    // c. select * from table order by ix  --> if lookup count < 10000
    // example2: id is primary key
    // d. select id,[*] from table order by id [limit];
    // example3: key ix (a, b)
    // e. select b from table order by a [limit]
    private static void matchSort(DingoIndexScanMatchRule rule, RelOptRuleCall call) {
        LogicalSort logicalSort = call.rel(0);
        LogicalDingoTableScan scan = call.rel(1);
        if (RuleUtils.validateDisableIndex(logicalSort.getHints())) {
            return;
        }
        DingoTable dingoTable = scan.getTable().unwrap(DingoTable.class);
        List<RelFieldCollation> relFieldCollationList = logicalSort.getCollation().getFieldCollations();
        if (relFieldCollationList.size() != 1) {
            return;
        }
        RelFieldCollation relFieldCollation = relFieldCollationList.get(0);
        int keepSerialOrder = RuleUtils.getSerialOrder(relFieldCollation);
        if (RuleUtils.preventRemoveOrder(keepSerialOrder)) {
            return;
        }

        int collationIndex = relFieldCollationList.get(0).getFieldIndex();
        int colIndex = scan.getSelection().get(collationIndex);
        assert dingoTable != null;
        Column orderCol;
        if (colIndex < dingoTable.getTable().columns.size()) {
            orderCol = dingoTable.getTable().columns.get(colIndex);
            if (RuleUtils.matchRemoveSort(orderCol.primaryKeyIndex, dingoTable.getTable().partitionStrategy)) {
                scan.setKeepSerialOrder(keepSerialOrder);
                call.transformTo(logicalSort.copy(logicalSort.getTraitSet(), scan,
                    new DingoRelCollationImpl(ImmutableList.of(), true, keepSerialOrder)));
                return;
            }
        } else {
            return;
        }

        if (RuleUtils.matchTablePrimary(logicalSort)) {
            return;
        }

        int[] ixs = scan.getSelection().getMappings();
        List<Integer> ixList = new ArrayList<>();
        for (int i : ixs) {
            ixList.add(i);
        }

        Result result = getResult(scan, ixList, scan.getTable());
        if (!(scan.getFilter() == null && result.matchIndex)) {
            return;
        }

        int matchSortIx = result.matchIndexTable.getColumns().indexOf(orderCol);
        if (matchSortIx < 0) {
            return;
        }
        Column matchOrderIxCol = result.matchIndexTable.getColumns().get(matchSortIx);
        if (!RuleUtils.matchRemoveSort(matchOrderIxCol.primaryKeyIndex, result.matchIndexTable.partitionStrategy)) {
            return;
        }
        RelCollation relCollation = RelCollationImpl.of(new ArrayList<>());

        List<Integer> indexSelectionList = result.matchIndexTable.getColumns().stream()
            .map(dingoTable.getTable().columns::indexOf)
            .collect(Collectors.toList());
        TupleMapping tupleMapping = TupleMapping.of(
            indexSelectionList
        );
        boolean needLookup = isNeedLookUp(scan.getSelection(), tupleMapping, dingoTable.getTable().columns.size());
        LogicalIndexFullScan indexFullScan = new LogicalIndexFullScan(
            scan.getCluster(),
            scan.getTraitSet(),
            scan.getHints(),
            scan.getTable(),
            scan.getFilter(),
            scan.getSelection(),
            result.matchIndexTable,
            result.indexId,
            result.selectionIxList,
            scan.isPushDown(),
            needLookup,
            keepSerialOrder
            );
        LogicalSort newSort = (LogicalSort) logicalSort.copy(logicalSort.getTraitSet(), indexFullScan, relCollation);
        call.transformTo(newSort);
    }

    public static void indexRange(DingoIndexScanMatchRule rule, RelOptRuleCall call) {
        LogicalProject logicalProject = call.rel(0);
        LogicalDingoTableScan scan = call.rel(1);
        if (RuleUtils.validateDisableIndex(scan.getHints())) {
            return;
        }
        LogicalProject logicalProject1 = getLogicalProject(scan, logicalProject);
        if (logicalProject1 == null) {
            return;
        }

        call.transformTo(logicalProject1);
    }

    @Nullable
    private static LogicalProject getLogicalProject(LogicalDingoTableScan scan, LogicalProject logicalProject) {
        RexNode rexNode = RexUtil.toDnf(scan.getCluster().getRexBuilder(), scan.getFilter());
        IndexRangeVisitor indexRangeVisitor = new IndexRangeVisitor(scan.getCluster().getRexBuilder());
        IndexRangeMapSet<Integer, RexNode> ixV = rexNode.accept(indexRangeVisitor);

        Map<CommonId, Table> indexTdMap = getScalaIndices(scan.getTable());

        if (indexTdMap.isEmpty()) {
            return getDocumentIndexScan(scan, logicalProject, ixV, rexNode);
        }
        final Table table = Objects.requireNonNull(scan.getTable().unwrap(DingoTable.class)).getTable();
        Map<CommonId, Set> indexSetMap = filterScalarIndex(ixV, indexTdMap, scan.getSelection(), table);
        if (indexSetMap == null || indexSetMap.isEmpty()) {
            return getDocumentIndexScan(scan, logicalProject, ixV, rexNode);
        }
        CommonId ixId;
        Table indexTd;
        if (indexSetMap.size() == 1) {
            Optional<Map.Entry<CommonId, Set>> ix = indexSetMap.entrySet().stream().findFirst();
            ixId = ix.get().getKey();
            indexTd = indexTdMap.get(ixId);
        } else {
            return null;
        }
        List<Integer> secList = getSelectionList(scan.getFilter(), logicalProject.getProjects());
        List<Column> columnNames = indexTd.getColumns();
        TupleMapping tupleMapping = TupleMapping.of(
            columnNames.stream().map(table.columns::indexOf).collect(Collectors.toList())
        );
        boolean lookup = isNeedLookUp(TupleMapping.of(secList), tupleMapping, table.columns.size());

        LogicalIndexRangeScan logicalIndexRangeScan = new LogicalIndexRangeScan(scan.getCluster(),
            scan.getTraitSet(),
            scan.getHints(),
            scan.getTable(),
            scan.getFilter(),
            scan.getSelection(),
            indexTd,
            ixId,
            scan.isPushDown(),
            lookup,
            0
        );
        return new LogicalProject(
            logicalProject.getCluster(),
            logicalProject.getTraitSet(),
            logicalProject.getHints(),
            logicalIndexRangeScan,
            logicalProject.getProjects(),
            logicalProject.getRowType(),
            logicalProject.getVariablesSet()
        );
    }

    private static LogicalProject getDocumentIndexScan(LogicalDingoTableScan scan, LogicalProject logicalProject,
                                                       IndexRangeMapSet<Integer, RexNode> ixV, RexNode rexNode) {
        // document filter
        if (GlobalVariablesUtil.getEnableDocumentScanFilter((DingoParserContext) scan.getCluster().getPlanner().getContext())) {
            Map<CommonId, Table> indexTdMap = getDocumentIndices(scan, scan.getTable());
            if (indexTdMap.isEmpty()) {
                return null;
            } else {
                final Table table = Objects.requireNonNull(scan.getTable().unwrap(DingoTable.class)).getTable();
                Map<CommonId, Set> indexSetMap = filterDocumentIndex(ixV, indexTdMap, scan.getSelection(), table);
                if (indexSetMap == null || indexSetMap.isEmpty()) {
                    return null;
                }
                CommonId ixId;
                Table indexTd;
                if (indexSetMap.size() == 1) {
                    Optional<Map.Entry<CommonId, Set>> ix = indexSetMap.entrySet().stream().findFirst();
                    ixId = ix.get().getKey();
                    indexTd = indexTdMap.get(ixId);
                } else {
                    return null;
                }

                Set<Map<Integer, RexNode>> set = ixV.getSet();
                if (set == null || set.isEmpty()) {
                    return null;
                }
                TupleMapping selection = scan.getSelection();
                List<Column> ixColList = indexTd.getColumns();
                boolean match = true;
                outer:for (Map<Integer, RexNode> map : set) {
                    for (int k : map.keySet()) {
                        int originIndex = (selection == null ? k : selection.get(k));
                        Column column = table.getColumns().get(originIndex);
                        // match primary key
                        if (column.primaryKeyIndex == 0) {
                            return null;
                        }
                        if (ixColList.indexOf(column) == 0) {
                            match = false;
                        }
                        if (match) {
                            DingoType type = column.getType();
                            if (!(type instanceof StringType || type instanceof LongType || type instanceof DoubleType
                                || type instanceof TimestampType || type instanceof BooleanType)) {
                                match = false;
                                break outer;
                            }
                            if (type instanceof StringType) {
                                Properties properties = indexTd.getProperties();
                                String json = (String) properties.get("text_fields");
                                if (json == null) {
                                    match = false;
                                    break outer;
                                }
                                try {
                                    ObjectMapper JSON = new ObjectMapper();
                                    JsonNode jsonNode = JSON.readTree(json);
                                    Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
                                    boolean flag = false;
                                    while (fields.hasNext()) {
                                        Map.Entry<String, JsonNode> next = fields.next();
                                        json = json.replace(next.getKey(), next.getKey().toUpperCase());
                                        JsonNode tokenizer = next.getValue().get("tokenizer");
                                        if (tokenizer == null) {
                                            match = false;
                                            break outer;
                                        }
                                        if (!next.getKey().equalsIgnoreCase(column.getName())) {
                                            continue;
                                        }
                                        flag = true;
                                        String tokenType = next.getValue().get("tokenizer").get("type").asText();
                                        if (!tokenType.equalsIgnoreCase("raw")) {
                                            match = false;
                                            break outer;
                                        }
                                    }
                                    if (!flag) {
                                        match = false;
                                        break;
                                    }
                                } catch (Exception e) {
                                    match = false;
                                    break outer;
                                }
                            }
                        }
                    }
                }
                if (!match) {
                    return null;
                }

                List<Integer> secList = getSelectionList(scan.getFilter(), logicalProject.getProjects());
                List<Column> columnNames = indexTd.getColumns();
                TupleMapping tupleMapping = TupleMapping.of(
                    columnNames.stream().map(table.columns::indexOf).collect(Collectors.toList())
                );
                boolean lookup = isNeedLookUp(TupleMapping.of(secList), tupleMapping, table.columns.size());

                DocumentScanFilterVisitor documentScanFilterVisitor = new DocumentScanFilterVisitor(
                    scan.getCluster().getRexBuilder(),
                    DocumentScanFilterOb.builder()
                        .match(false)
                        .queryStr("")
                        .columns(table.columns)
                        .build()
                );
                DocumentScanFilterOb accept = rexNode.accept(documentScanFilterVisitor);
                boolean flag = accept.isMatch();
                String queryString = accept.getQueryStr();
                if (flag) {
                    LogicalDocumentScanFilter logicalDocumentScanFilter = new LogicalDocumentScanFilter(
                        scan.getCluster(),
                        scan.getTraitSet(),
                        scan.getHints(),
                        scan.getTable(),
                        scan.getFilter(),
                        scan.getSelection(),
                        indexTd,
                        ixId,
                        scan.isPushDown(),
                        lookup,
                        0,
                        queryString
                    );
                    return new LogicalProject(
                        logicalProject.getCluster(),
                        logicalProject.getTraitSet(),
                        logicalProject.getHints(),
                        logicalDocumentScanFilter,
                        logicalProject.getProjects(),
                        logicalProject.getRowType(),
                        logicalProject.getVariablesSet()
                    );
                }
            }
        }

        return null;
    }

    public static void indexRangeAsc(DingoIndexScanMatchRule rule, RelOptRuleCall call) {
        LogicalSort logicalSort = call.rel(0);
        LogicalProject logicalProject = call.rel(1);
        boolean disableIndex = !logicalProject.getHints().isEmpty()
            && "disable_index".equalsIgnoreCase(logicalProject.getHints().get(0).hintName);
        if (disableIndex) {
            return;
        }
        if (RuleUtils.matchTablePrimary(logicalSort)) {
            return;
        }

        List<RelFieldCollation> relFieldCollationList = logicalSort.getCollation().getFieldCollations();
        if (relFieldCollationList.size() != 1) {
            return;
        }
        LogicalIndexRangeScan logicalIndexRangeScan = call.rel(2);
        Table table = logicalIndexRangeScan.getTable().unwrap(DingoTable.class).getTable();
        Column orderCol = null;
        RelFieldCollation relFieldCollation = relFieldCollationList.get(0);
        int i = relFieldCollation.getFieldIndex();
        RexNode rexNode = logicalProject.getProjects().get(i);
        IndexOpExpr indexOpExpr = (IndexOpExpr) RexConverter.convert(rexNode);
        int ix = getIndexByExpr(indexOpExpr);
        if (ix >= 0) {
            orderCol = table.getColumns().get(ix);
        }

        if (orderCol == null) {
            return;
        }
        if (validateNotRemoveSort(logicalIndexRangeScan.getIndexTable(), orderCol)) {
            return;
        }
        int keepSerialOrder = RuleUtils.getSerialOrder(relFieldCollation);
        if (RuleUtils.preventRemoveOrder(keepSerialOrder)) {
            return;
        }
        RelCollation relCollation = RelCollationImpl.of(new ArrayList<>());
        logicalIndexRangeScan.setKeepSerialOrder(keepSerialOrder);
        LogicalSort newSort = (LogicalSort) logicalSort.copy(
            logicalSort.getTraitSet(), logicalProject, relCollation
        );
        call.transformTo(newSort);
    }

    // index ix1(a,b,c)
    // b > v || b == v || b < v
    // c > v || c == v || c < v
    public static void nonLeftmostMatching(DingoIndexScanMatchRule rule, RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        boolean disableIndex = !project.getHints().isEmpty()
            && "disable_index".equalsIgnoreCase(project.getHints().get(0).hintName);
        if (disableIndex) {
            return;
        }
        LogicalDingoTableScan scan = call.rel(1);

        RelNode relNode = getIndexFullScanRelNode(project, scan);
        if (relNode == null) {
            return;
        }

        call.transformTo(relNode);
    }

    public static void nonLeftmostMatchingOrder(DingoIndexScanMatchRule rule, RelOptRuleCall call) {
        LogicalSort logicalSort = call.rel(0);
        LogicalProject project = call.rel(1);

        if (RuleUtils.validateDisableIndex(project.getHints())) {
            return;
        }

        if (RuleUtils.matchTablePrimary(logicalSort)) {
            return;
        }

        List<RelFieldCollation> relFieldCollationList = logicalSort.getCollation().getFieldCollations();
        if (relFieldCollationList.size() != 1) {
            return;
        }
        RelFieldCollation relFieldCollation = relFieldCollationList.get(0);
        int i = relFieldCollation.getFieldIndex();
        RexNode rexNode = project.getProjects().get(i);
        IndexOpExpr indexOpExpr = (IndexOpExpr) RexConverter.convert(rexNode);
        int ix = getIndexByExpr(indexOpExpr);
        Column orderCol;
        LogicalIndexFullScan logicalIndexFullScan = call.rel(2);
        if (ix >= 0 && ix < logicalIndexFullScan.getIndexTable().getColumns().size()) {
            orderCol = logicalIndexFullScan.getIndexTable().getColumns().get(ix);
        } else {
            return;
        }
        if (validateNotRemoveSort(logicalIndexFullScan.getIndexTable(), orderCol)) {
            return;
        }
        int keepSerialOrder = RuleUtils.getSerialOrder(relFieldCollation);
        if (RuleUtils.preventRemoveOrder(keepSerialOrder)) {
            return;
        }
        RelCollation relCollation = RelCollationImpl.of(new ArrayList<>());
        logicalIndexFullScan.setKeepSerialOrder(keepSerialOrder);
        LogicalSort newSort = (LogicalSort) logicalSort.copy(
            logicalSort.getTraitSet(), project, relCollation
        );
        call.transformTo(newSort);
    }

    @Nullable
    private static RelNode getIndexFullScanRelNode(LogicalProject project, LogicalDingoTableScan scan) {
        final List<Integer> selectedColumns = new ArrayList<>();
        final RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
            @Override
            public @Nullable Void visitInputRef(@NonNull RexInputRef inputRef) {
                if (!selectedColumns.contains(inputRef.getIndex())) {
                    selectedColumns.add(inputRef.getIndex());
                }
                return null;
            }

        };
        List<RexNode> projects = project.getProjects();
        RexNode filter = scan.getFilter();
        visitor.visitEach(projects);
        if (filter != null) {
            filter.accept(visitor);
        }

        // extract filter selection filterSec;
        // extract project selection projectSec;
        // filterSec && projectSec
        Result result = getResult(scan, selectedColumns, scan.getTable());
        if (!(scan.getFilter() != null && result.matchIndex)) {
            return null;
        }
        Table table = Objects.requireNonNull(scan.getTable().unwrap(DingoTable.class)).getTable();
        TupleMapping selection = scan.getSelection();
        List<Column> ixColList = result.matchIndexTable.getColumns();
        RexNode rexNode = RexUtil.toDnf(scan.getCluster().getRexBuilder(), scan.getFilter());
        IndexRangeVisitor indexRangeVisitor = new IndexRangeVisitor(scan.getCluster().getRexBuilder());
        IndexRangeMapSet<Integer, RexNode> indexValueMapSet = rexNode.accept(indexRangeVisitor);
        Set<Map<Integer, RexNode>> set = indexValueMapSet.getSet();

        if (set == null || set.isEmpty()) {
            return null;
        }
        boolean match = true;
        outer:for (Map<Integer, RexNode> map : set) {
            for (int k : map.keySet()) {
                int originIndex = (selection == null ? k : selection.get(k));
                Column column = table.getColumns().get(originIndex);
                // match primary key
                if (column.primaryKeyIndex == 0) {
                    return null;
                }
                if (ixColList.indexOf(column) == 0 && !result.isDocumentIndex) {
                    match = false;
                }
                if (result.isDocumentIndex && match) {
                    DingoType type = column.getType();
                    if (!(type instanceof StringType || type instanceof LongType || type instanceof DoubleType ||
                        type instanceof TimestampType || type instanceof BooleanType)) {
                        match = false;
                        break outer;
                    }
                    if (type instanceof StringType) {
                        Properties properties = result.matchIndexTable.getProperties();
                        String json = (String) properties.get("text_fields");
                        if (json == null) {
                            match = false;
                            break outer;
                        }
                        try {
                            ObjectMapper JSON = new ObjectMapper();
                            JsonNode jsonNode = JSON.readTree(json);
                            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
                            boolean flag = false;
                            while (fields.hasNext()) {
                                Map.Entry<String, JsonNode> next = fields.next();
                                json = json.replace(next.getKey(), next.getKey().toUpperCase());
                                JsonNode tokenizer = next.getValue().get("tokenizer");
                                if (tokenizer == null) {
                                    match = false;
                                    break outer;
                                }
                                if (!next.getKey().equalsIgnoreCase(column.getName())) {
                                    continue;
                                }
                                flag = true;
                                String tokenType = next.getValue().get("tokenizer").get("type").asText();
                                if (!tokenType.equalsIgnoreCase("raw")) {
                                    match = false;
                                    break outer;
                                }
                            }
                            if (!flag) {
                                match = false;
                                break;
                            }
                        } catch (Exception e) {
                            match = false;
                            break outer;
                        }
                    }
                }
            }
        }
        if (!match) {
            return null;
        }
        selectedColumns.sort(Comparator.naturalOrder());
        Mapping mapping = Mappings.target(selectedColumns, scan.getRowType().getFieldCount());
        final List<RexNode> newProjectRexNodes = RexUtil.apply(mapping, project.getProjects());

        TupleMapping finalSelection = TupleMapping.of(selectedColumns);
        List<Integer> indexSelectionList = result.matchIndexTable.getColumns().stream()
            .map(table.columns::indexOf).collect(Collectors.toList());
        TupleMapping tupleMapping = TupleMapping.of(
            indexSelectionList
        );
        boolean needLookup = isNeedLookUp(finalSelection, tupleMapping, table.columns.size());

        RelNode relNode;
        boolean flag = false;
        String queryString = "";
        if (result.isDocumentIndex) {
            DocumentScanFilterVisitor documentScanFilterVisitor = new DocumentScanFilterVisitor(
                scan.getCluster().getRexBuilder(),
                DocumentScanFilterOb.builder()
                    .match(false)
                    .queryStr("")
                    .columns(table.columns)
                    .build()
            );
            DocumentScanFilterOb accept = rexNode.accept(documentScanFilterVisitor);
            flag = accept.isMatch();
            queryString = accept.getQueryStr();
        }
        if (result.isDocumentIndex && flag) {
            LogicalDocumentScanFilter indexScan = new LogicalDocumentScanFilter(
                scan.getCluster(),
                scan.getTraitSet(),
                scan.getHints(),
                scan.getTable(),
                scan.getFilter(),
                finalSelection,
                result.matchIndexTable,
                result.indexId,
                scan.isPushDown(),
                needLookup,
                0,
                queryString);
            relNode = new LogicalProject(
                project.getCluster(),
                project.getTraitSet(),
                project.getHints(),
                indexScan,
                newProjectRexNodes,
                project.getRowType(),
                project.getVariablesSet()
            );
        } else {
            LogicalIndexFullScan indexFullScan = new LogicalIndexFullScan(
                scan.getCluster(),
                scan.getTraitSet(),
                scan.getHints(),
                scan.getTable(),
                scan.getFilter(),
                finalSelection,
                result.matchIndexTable,
                result.indexId,
                result.selectionIxList,
                scan.isPushDown(),
                needLookup,
                0);
            if (RexUtil.isIdentity(newProjectRexNodes, scan.getSelectedType())) {
                relNode = indexFullScan;
            } else {
                relNode = new LogicalProject(
                    project.getCluster(),
                    project.getTraitSet(),
                    project.getHints(),
                    indexFullScan,
                    newProjectRexNodes,
                    project.getRowType(),
                    project.getVariablesSet()
                );
            }
        }
        return relNode;
    }

    @NonNull
    private static Result getResult(LogicalDingoTableScan scan, List<Integer> ixList, RelOptTable relOptTable) {
        boolean matchIndex = false;
        Table matchIndexTable = null;
        CommonId indexId = null;
        boolean isDocumentIndex = false;

        Table table = Objects.requireNonNull(relOptTable.unwrap(DingoTable.class)).getTable();
        Map<CommonId, Table> indexTdMap = getScalaIndices(relOptTable);
        for (Map.Entry<CommonId, Table> index : indexTdMap.entrySet()) {
            Table indexTable = index.getValue();
            if (!"range".equalsIgnoreCase(indexTable.getPartitionStrategy())) {
                continue;
            }
            List<Integer> indices = indexTable.getColumns()
                .stream().map(table.getColumns()::indexOf)
                .collect(Collectors.toList());
            if (indices.containsAll(ixList)) {
                matchIndex = true;
                matchIndexTable = indexTable;
                indexId = index.getKey();
                break;
            }
        }
        if (!matchIndex) {
            indexTdMap = getDocumentIndices(scan, relOptTable);
            for (Map.Entry<CommonId, Table> index : indexTdMap.entrySet()) {
                Table indexTable = index.getValue();
                List<Integer> indices = indexTable.getColumns()
                    .stream().map(table.getColumns()::indexOf)
                    .collect(Collectors.toList());
                if (indices.containsAll(ixList)) {
                    matchIndex = true;
                    isDocumentIndex = true;
                    matchIndexTable = indexTable;
                    indexId = index.getKey();
                    break;
                }
            }
        }
        return new Result(matchIndex, matchIndexTable, indexId, ixList, isDocumentIndex);
    }

    private static class Result {
        public final boolean matchIndex;
        public final Table matchIndexTable;
        public final CommonId indexId;
        public final List<Integer> selectionIxList;
        public final boolean isDocumentIndex;

        public Result(boolean sortIndex, Table matchIndexTable, CommonId indexId,
                      List<Integer> selectionIndexList, boolean isDocumentIndex) {
            this.matchIndex = sortIndex;
            this.matchIndexTable = matchIndexTable;
            this.indexId = indexId;
            this.selectionIxList = selectionIndexList;
            this.isDocumentIndex = isDocumentIndex;
        }
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        config.matchHandler().accept(this, relOptRuleCall);
    }

    public static @Nullable Map<CommonId, Set> filterDocumentIndex(
        @NonNull IndexRangeMapSet<Integer, RexNode> mapSet,
        @NonNull Map<CommonId, Table> indexTdMap,
        TupleMapping selection,
        Table td
    ) {
        Set<Map<Integer, RexNode>> set = mapSet.getSet();
        if (set == null) {
            return null;
        }
        List<Column> columnList;
        List<Integer> indices;
        Map<CommonId, Set> indexMap = new HashMap<>();
        boolean matchIndex;
        for (Map<Integer, RexNode> map : set) {
            matchIndex = false;
            for (Map.Entry<CommonId, Table> index : indexTdMap.entrySet()) {
                columnList = index.getValue().getColumns();
                indices = columnList.stream().map(td.getColumns()::indexOf).collect(Collectors.toList());
                Map<Integer, RexNode> newMap = new HashMap<>(indices.size());
                boolean allMatch = true;
                for (int k : map.keySet()) {
                    if (selection != null && k >= selection.size()) {
                        return null;
                    }
                    int originIndex = (selection == null ? k : selection.get(k));
                    if (indices.contains(originIndex)) {
                        newMap.put(indices.indexOf(originIndex), map.get(k));
                    } else {
                        allMatch = false;
                        break;
                    }
                }
                // Leftmost matching principle
                // index a columns(b, c, d)  -> where b = v  ==> matched index a
                if (allMatch) {
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

    public static @Nullable Map<CommonId, Set> filterScalarIndex(
        @NonNull IndexRangeMapSet<Integer, RexNode> mapSet,
        @NonNull Map<CommonId, Table> indexTdMap,
        TupleMapping selection,
        Table td
    ) {
        Set<Map<Integer, RexNode>> set = mapSet.getSet();
        if (set == null) {
            return null;
        }
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
                boolean allMatch = true;
                for (int k : map.keySet()) {
                    if (selection != null && k >= selection.size()) {
                        return null;
                    }
                    int originIndex = (selection == null ? k : selection.get(k));
                    if (indices.contains(originIndex)) {
                        newMap.put(indices.indexOf(originIndex), map.get(k));
                    } else {
                        allMatch = false;
                        break;
                    }
                }
                // Leftmost matching principle
                // index a columns(b, c, d)  -> where b = v  ==> matched index a
                if (newMap.containsKey(0) && allMatch) {
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

    public static List<Integer> getSelectionList(RexNode filter, List<RexNode> exprList) {
        List<Integer> secList = new ArrayList<>();
        final RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
            @Override
            public @Nullable Void visitInputRef(@NonNull RexInputRef inputRef) {
                if (!secList.contains(inputRef.getIndex())) {
                    secList.add(inputRef.getIndex());
                }
                return null;
            }
        };
        if (filter != null) {
            filter.accept(visitor);
        }
        if (exprList != null) {
            visitor.visitEach(exprList);
        }
        return secList;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {

        DingoIndexScanMatchRule.Config PROJECT_SORT = ImmutableDingoIndexScanMatchRule.Config.builder()
            .description("DingoIndexScanMatchRule(ProjectSort)")
            .operandSupplier(b0 ->
                b0.operand(LogicalSort.class).oneInput(b1 ->
                    b1.operand(LogicalProject.class).oneInput(b2 ->
                      b2.operand(LogicalDingoTableScan.class)
                        .predicate(scan -> scan.getFilter() == null
                            && !(scan instanceof LogicalIndexFullScan)).noInputs()
                    )
                )
            )
            .matchHandler(DingoIndexScanMatchRule::matchProjectSortOrder)
            .build();
        DingoIndexScanMatchRule.Config SORT = ImmutableDingoIndexScanMatchRule.Config.builder()
            .description("DingoIndexScanMatchRule(Sort)")
            .operandSupplier(b0 ->
                b0.operand(LogicalSort.class).oneInput(b1 ->
                    b1.operand(LogicalDingoTableScan.class)
                      .predicate(scan -> scan.getFilter() == null && !(scan instanceof LogicalIndexFullScan)).noInputs()
                )
            )
            .matchHandler(DingoIndexScanMatchRule::matchSort)
            .build();

        DingoIndexScanMatchRule.Config NONLEFT = ImmutableDingoIndexScanMatchRule.Config.builder()
            .description("DingoIndexScanMatchRule(nonLeft)")
            .operandSupplier(b0 ->
                 b0.operand(LogicalProject.class).oneInput(b1 ->
                     b1.operand(LogicalDingoTableScan.class).predicate(scan -> scan.getFilter() != null
                       && scan.getRealSelection() == null && !(scan instanceof LogicalIndexFullScan)).noInputs()
                 )
            )
            .matchHandler(DingoIndexScanMatchRule::nonLeftmostMatching)
            .build();

        DingoIndexScanMatchRule.Config NONLEFT_ORDER = ImmutableDingoIndexScanMatchRule.Config.builder()
            .description("DingoIndexScanMatchRule(nonLeftOrder)")
            .operandSupplier(b0 ->
                b0.operand(LogicalSort.class).oneInput(b1 ->
                  b1.operand(LogicalProject.class).oneInput(b2 ->
                    b2.operand(LogicalIndexFullScan.class).noInputs()
                )
                )
            )
            .matchHandler(DingoIndexScanMatchRule::nonLeftmostMatchingOrder)
            .build();

        DingoIndexScanMatchRule.Config INDEXSCAN = ImmutableDingoIndexScanMatchRule.Config.builder()
            .description("DingoIndexScanMatchRule(indexScan)")
            .operandSupplier(b0 ->
                b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(LogicalDingoTableScan.class).predicate(scan -> scan.getFilter() != null
                              && !(scan instanceof LogicalIndexRangeScan)
                              && scan.getRealSelection() == null).noInputs())
            )
            .matchHandler(DingoIndexScanMatchRule::indexRange)
            .build();

        DingoIndexScanMatchRule.Config INDEXSCAN_SORT_ORDER = ImmutableDingoIndexScanMatchRule.Config.builder()
            .description("DingoIndexScanMatchRule(indexScanOrder)")
            .operandSupplier(b0 ->
                b0.operand(LogicalSort.class).oneInput(b1 ->
                    b1.operand(LogicalProject.class).oneInput(b2 ->
                        b2.operand(LogicalIndexRangeScan.class).noInputs())
                    )
            )
            .matchHandler(DingoIndexScanMatchRule::indexRangeAsc)
            .build();

        @Override
        default DingoIndexScanMatchRule toRule() {
            return new DingoIndexScanMatchRule(this);
        }

        @Value.Parameter
        MatchHandler<DingoIndexScanMatchRule> matchHandler();
    }
}
