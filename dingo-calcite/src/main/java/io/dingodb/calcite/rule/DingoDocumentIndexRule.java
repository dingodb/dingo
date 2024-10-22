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

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByIndexMerge;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoGetDocumentPreFilter;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoDocument;
import io.dingodb.calcite.rel.LogicalDingoDocument;
import io.dingodb.calcite.rel.DocumentStreamConvertor;
import io.dingodb.calcite.rel.dingo.DingoStreamingConverter;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.utils.IndexValueMapSet;
import io.dingodb.calcite.utils.IndexValueMapSetVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Pair;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rel.LogicalDingoTableScan.dispatchDistanceCondition;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.eliminateSpecialCast;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.filterIndices;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.filterScalarIndices;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.getScalaIndices;

@Slf4j
@Value.Enclosing
public class DingoDocumentIndexRule extends RelRule<RelRule.Config> {

    /**
     * Creates a RelRule.
     *
     * @param config config
     */
    protected DingoDocumentIndexRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        DingoDocument document = call.rel(0);
        RelNode relNode = getDingoGetDocumentPreFilter(document.getFilter(), document, false);
        if (relNode == null) {
            return;
        }
        call.transformTo(relNode);
    }

    public static RelNode getDingoGetDocumentPreFilter(RexNode condition, LogicalDingoDocument document, boolean forJoin) {
        DingoTable dingoTable = document.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        TupleMapping selection = getDefaultSelection(dingoTable);

        if (condition != null) {
            dispatchDistanceCondition(condition, selection, dingoTable);
        }

        // if filter matched point get by primary key, then DingoGetByKeys priority highest
        Pair<Integer, Integer> textIdPair = getTextIdIndex(dingoTable);
        assert textIdPair != null;
        RelTraitSet traitSet = document.getTraitSet().replace(DingoRelStreaming.of(document.getTable()));
        boolean preFilter = document.getHints() != null
            && !document.getHints().isEmpty()
            && "text_search_pre".equalsIgnoreCase(document.getHints().get(0).hintName);

        // document filter match primary point get
        RelNode relNode = prePrimaryOrScalarPlan(condition, document,textIdPair, traitSet, selection, preFilter);
        if (relNode != null) {
            return relNode;
        }

        if (!preFilter && !forJoin) {
            return null;
        }

        // pre filtering
        //Step1：Table scan to find the target original columns and store them into cache
        //Step2: Text search with document id, returns document id and score
        //Step3: Merge cache data and document score with document id

        DingoTableScan dingoTableScan = new DingoTableScan(document.getCluster(),
            traitSet,
            ImmutableList.of(),
           document.getTable(),
            condition,
            selection,
            null,
            null,
            null,
            true,
            false
        );

        DocumentStreamConvertor documentStreamConvertor = new DocumentStreamConvertor(
           document.getCluster(),
           document.getTraitSet(),
            dingoTableScan,
           document.getIndexTableId(),
            textIdPair.getKey(),
           document.getIndexTable(),
            false);
        return new DingoGetDocumentPreFilter(
           document.getCluster(),
            traitSet,
           documentStreamConvertor,
            condition,
           document.getTable(),
           document.getOperands(),
            textIdPair.getKey(),
            textIdPair.getValue(),
           document.getIndexTableId(),
           document.getSelection(),
           document.getIndexTable()
        );
    }

    private static DingoGetByIndex preScalarRelNode(LogicalDingoDocument dingoDocument,
                                         IndexValueMapSet<Integer, RexNode> indexValueMapSet,
                                         Table td,
                                         TupleMapping selection,
                                         RexNode condition) {
        Map<CommonId, Table> indexTdMap = getScalaIndices(dingoDocument.getTable());

        if (indexTdMap.isEmpty()) {
            return null;
        }
        Map<CommonId, Set> indexSetMap = filterScalarIndices(
            indexValueMapSet,
            indexTdMap,
            selection,
            td);
        if (indexSetMap == null) {
            return null;
        }
        if (indexSetMap.size() > 1) {
            return new DingoGetByIndexMerge(
                dingoDocument.getCluster(),
                dingoDocument.getTraitSet(),
                ImmutableList.of(),
                dingoDocument.getTable(),
                condition,
                selection,
                false,
                indexSetMap,
                indexTdMap,
                td.keyMapping()
            );
        } else {
            return new DingoGetByIndex(
                dingoDocument.getCluster(),
                dingoDocument.getTraitSet(),
                ImmutableList.of(),
                dingoDocument.getTable(),
                condition,
                selection,
                false,
                indexSetMap,
                indexTdMap
            );
        }
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoDocumentIndexRule.Config.builder()
            .description("DingoDocumentIndexRule")
            .operandSupplier(b0 ->
                b0.operand(DingoDocument.class).predicate(rel -> rel.getFilter() != null).noInputs()
            )
            .build();

        @Override
        default DingoDocumentIndexRule toRule() {
            return new DingoDocumentIndexRule(this);
        }
    }
    private static RelNode prePrimaryOrScalarPlan(
        RexNode condition,
        LogicalDingoDocument document,
        Pair<Integer, Integer> documentIdPair,
        RelTraitSet traitSet,
        TupleMapping selection,
        boolean preFilter) {
        if (condition == null) {
            return null;
        }
        DingoTable dingoTable = document.getTable().unwrap(DingoTable.class);
        RexNode rexNode = RexUtil.toDnf(document.getCluster().getRexBuilder(), condition);
        rexNode = eliminateSpecialCast(rexNode, document.getCluster().getRexBuilder());
        IndexValueMapSetVisitor visitor = new IndexValueMapSetVisitor(document.getCluster().getRexBuilder());
        IndexValueMapSet<Integer, RexNode> indexValueMapSet = rexNode.accept(visitor);
        assert dingoTable != null;
        final Table td = dingoTable.getTable();
        List<Integer> keyIndices = Arrays.stream(td.keyMapping().getMappings()).boxed().collect(Collectors.toList());

        Set<Map<Integer, RexNode>> keyMapSet = filterIndices(indexValueMapSet, keyIndices, selection);

        RelNode scan = null;
        if (keyMapSet != null) {
            scan = new DingoGetByKeys(
                document.getCluster(),
                document.getTraitSet(),
                ImmutableList.of(),
                document.getTable(),
                condition,
                selection,
                keyMapSet
            );
        } else if (preFilter) {
            scan = preScalarRelNode(document, indexValueMapSet, td, selection, condition);
        }

        if (scan == null) {
            return null;
        }
        DocumentStreamConvertor documentStreamConvertor = new DocumentStreamConvertor(
            document.getCluster(),
            document.getTraitSet(),
            scan,
            document.getIndexTableId(),
            documentIdPair.getKey(),
            document.getIndexTable(),
            false);
        DingoGetDocumentPreFilter dingoGetDocumentPreFilter = new DingoGetDocumentPreFilter(
            document.getCluster(),
            traitSet,
            documentStreamConvertor,
            condition,
            document.getTable(),
            document.getOperands(),
            documentIdPair.getKey(),
            documentIdPair.getValue(),
            document.getIndexTableId(),
            document.getSelection(),
            document.getIndexTable()
        );
        RelTraitSet traits = document.getCluster().traitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.ROOT);
        return new DingoStreamingConverter(document.getCluster(),
            traits, dingoGetDocumentPreFilter);
    }
    private static Pair<Integer, Integer> getTextIdIndex(DingoTable dingoTable) {
        List<IndexTable> indexes = dingoTable.getTable().getIndexes();
        for (IndexTable index : indexes) {

            if (index.getIndexType() != IndexType.DOCUMENT) {
                continue;
            }

            String documentIdColName = index.getColumns().get(0).getName();
            String documentColName = index.getColumns().get(1).getName();
            int documentIdIndex = 0;
            int documentIndex = 0;
            for (int i = 0; i < dingoTable.getTable().getColumns().size(); i ++) {
                Column column = dingoTable.getTable().getColumns().get(i);
                if (column.getName().equals(documentIdColName)) {
                    documentIdIndex = i;
                } else if (column.getName().equals(documentColName)) {
                    documentIndex = i;
                }
            }
            return Pair.of(documentIdIndex, documentIndex);
        }
        return null;
    }

    public static TupleMapping getDefaultSelection(DingoTable dingoTable) {
        int columnsCount = dingoTable.getTable().getColumns().size();
        int[] mappings = new int[columnsCount];
        for (int i = 0; i < columnsCount; i ++) {
            mappings[i] = i;
        }
        return TupleMapping.of(mappings);
    }

}
