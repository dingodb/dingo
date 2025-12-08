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
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.rel.logical.LogicalDocumentScanFilter;
import io.dingodb.calcite.rel.logical.LogicalIndexFullScan;
import io.dingodb.calcite.utils.DocumentScanFilterOb;
import io.dingodb.calcite.utils.DocumentScanFilterVisitor;
import io.dingodb.calcite.utils.IndexRangeMapSet;
import io.dingodb.calcite.utils.IndexRangeVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static io.dingodb.calcite.rule.DingoGetByIndexRule.getDocumentIndices;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.getScalaIndices;
import static io.dingodb.common.util.Utils.isNeedLookUp;
import static org.apache.calcite.sql.SqlKind.AND;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.IN;
import static org.apache.calcite.sql.SqlKind.IS_NULL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.OR;

@Slf4j
@Value.Enclosing
public class DingoIndexNonLeftMatchRule extends RelRule<DingoIndexNonLeftMatchRule.Config> {

    public static final Set<SqlKind> INDEX_KIND =
        EnumSet.of(
            IN, EQUALS, AND,
            LESS_THAN, GREATER_THAN,
            GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL,
            IS_NULL, OR);

    protected DingoIndexNonLeftMatchRule(Config config) {
        super(config);
    }

    // index ix1(a,b,c)
    // b > v || b == v || b < v
    // c > v || c == v || c < v
    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        boolean disableIndex = !project.getHints().isEmpty()
            && "disable_index".equalsIgnoreCase(project.getHints().get(0).hintName);
        if (disableIndex) {
            return;
        }
        LogicalDingoTableScan scan = call.rel(1);
        if (scan.getFilter() != null && !scan.getFilter().isA(INDEX_KIND)) {
            return;
        }
        RelNode relNode = getIndexFullScanRelNode(project, scan);
        if (relNode == null) {
            return;
        }

        call.transformTo(relNode);
        if (relNode.getInput(0) instanceof LogicalDocumentScanFilter) {
            call.getPlanner().prune(project);
        }
    }

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
        //visitor.visitEach(projects);
        if (filter != null) {
            filter.accept(visitor);
        }

        // extract filter selection filterSec;
        // extract project selection projectSec;
        // filterSec && projectSec
        DingoIndexScanMatchRule.Result result = getResult(scan, selectedColumns, scan.getTable());
        if (!(scan.getFilter() != null && result.matchIndex)) {
            return null;
        }
        Table table = Objects.requireNonNull(scan.getTable().unwrap(DingoTable.class)).getTable();
        TupleMapping selection = scan.getSelection();
        RexNode rexNode = RexUtil.toDnf(scan.getCluster().getRexBuilder(), scan.getFilter());
        IndexRangeVisitor indexRangeVisitor = new IndexRangeVisitor(scan.getCluster().getRexBuilder());
        IndexRangeMapSet<Integer, RexNode> indexValueMapSet = rexNode.accept(indexRangeVisitor);
        Set<Map<Integer, RexNode>> set = indexValueMapSet.getSet();

        if (set == null || set.isEmpty()) {
            return null;
        }
        boolean match = true;
        outer : for (Map<Integer, RexNode> map : set) {
            for (int k : map.keySet()) {
                int originIndex = (selection == null ? k : selection.get(k));
                Column column = table.getColumns().get(originIndex);
                // match primary key
                //if (column.primaryKeyIndex == 0) {
                //    return null;
                //}
                if (result.matchIndexTable.getColumnIndex(column) == 0 && !result.isDocumentIndex) {
                    match = false;
                }
                if (result.isDocumentIndex && match) {
                    DingoType type = column.getType();
                    if (!(type instanceof StringType || type instanceof LongType || type instanceof DoubleType
                        || type instanceof TimestampType || type instanceof BooleanType)) {
                        match = false;
                        break outer;
                    }
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
                            if (type instanceof StringType) {
                                String tokenType = next.getValue().get("tokenizer").get("type").asText();
                                if (!tokenType.equalsIgnoreCase("raw")) {
                                    match = false;
                                    break outer;
                                }
                            }
                            break ;
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
        if (!match) {
            return null;
        }
        visitor.visitEach(projects);
        selectedColumns.sort(Comparator.naturalOrder());
        Mapping mapping = Mappings.target(selectedColumns, scan.getRowType().getFieldCount());
        final List<RexNode> newProjectRexNodes = RexUtil.apply(mapping, project.getProjects());

        TupleMapping finalSelection = TupleMapping.of(selectedColumns);
        List<Integer> indexSelectionList = table.getColumnIndices2(result.matchIndexTable.getColumns());
        TupleMapping tupleMapping = TupleMapping.of(indexSelectionList);
        boolean needLookup = isNeedLookUp(finalSelection, tupleMapping, table.columns.size());

        RelNode relNode;
        if (result.isDocumentIndex) {
            try {
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
                    return relNode;
                } else {
                    return null;
                }
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
            }
            return null;
        }
        if (scan.getFilter().isA(OR)) {
            return null;
        }
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
        return relNode;
    }

    public static DingoIndexScanMatchRule.Result getResult(
        LogicalDingoTableScan scan, List<Integer> ixList, RelOptTable relOptTable
    ) {
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
            List<Integer> indices = table.getColumnIndices2(indexTable.getColumns());
            if (indices.containsAll(ixList)) {
                matchIndex = true;
                matchIndexTable = indexTable;
                indexId = index.getKey();
                break;
            } else if (ixList.stream().anyMatch(indices::contains)) {
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
                List<Integer> indices = table.getColumnIndices2(indexTable.getColumns());
                if (indices.containsAll(ixList)) {
                    matchIndex = true;
                    isDocumentIndex = true;
                    matchIndexTable = indexTable;
                    indexId = index.getKey();
                    break;
                }
            }
        }
        return new DingoIndexScanMatchRule.Result(matchIndex, matchIndexTable, indexId, ixList, isDocumentIndex);
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {

        DingoIndexNonLeftMatchRule.Config DEFAULT = ImmutableDingoIndexNonLeftMatchRule.Config.builder()
            .description("DingoIndexNonLeftMatchRule(nonLeft)")
            .operandSupplier(b0 ->
                b0.operand(LogicalProject.class).oneInput(b1 ->
                    b1.operand(LogicalDingoTableScan.class).predicate(scan -> scan.getFilter() != null
                        && scan.getRealSelection() == null && !(scan instanceof LogicalIndexFullScan)).noInputs()
                )
            )
            .build();

        @Override
        default DingoIndexNonLeftMatchRule toRule() {
            return new DingoIndexNonLeftMatchRule(this);
        }

    }
}
