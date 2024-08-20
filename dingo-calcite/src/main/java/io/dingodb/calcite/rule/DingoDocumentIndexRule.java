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
//        RelNode relNode = getDingoGetDocumentByToken(document.getFilter(), document, false);
        RelNode relNode = null;
        if (relNode == null) {
            return;
        }
        call.transformTo(relNode);
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

    private static Pair<Integer, Integer> getDocumentIndex(DingoTable dingoTable, int dimension) {
        List<IndexTable> indexes = dingoTable.getTable().getIndexes();
        for (IndexTable index : indexes) {

            if (!index.getIndexType().isVector) {
                continue;
            }

            int dimension1 = Integer.parseInt(index.getProperties().getProperty("dimension"));
            if (dimension != dimension1) {
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
