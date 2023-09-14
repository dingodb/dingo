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
import io.dingodb.calcite.rel.DingoFilter;
import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoGetVectorByDistance;
import io.dingodb.calcite.rel.DingoStreamingConverter;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.rel.VectorStreamConvertor;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.utils.IndexValueMapSet;
import io.dingodb.calcite.utils.IndexValueMapSetVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.dingodb.calcite.rule.DingoGetByIndexRule.filterIndices;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.filterScalarIndices;
import static io.dingodb.calcite.rule.DingoGetByIndexRule.getScalaIndices;

@Slf4j
@Value.Enclosing
public class DingoVectorIndexRule extends RelRule<RelRule.Config> {

    /**
     * Creates a RelRule.
     *
     * @param config config
     */
    protected DingoVectorIndexRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        DingoFilter filter = call.rel(0);
        DingoVector vector = call.rel(1);
        if (filter.getHints().size() == 0) {
            return;
        } else {
            if (!"vector_pre".equalsIgnoreCase(filter.getHints().get(0).hintName)) {
                return;
            }
        }
        DingoTable dingoTable = vector.getTable().unwrap(DingoTable.class);
        assert dingoTable != null;
        TupleMapping selection = getDefaultSelection(dingoTable);

        // if filter matched point get by primary key, then DingoGetByKeys priority highest
        Pair<Integer, Integer> vectorIdPair = getVectorIndex(dingoTable);
        assert vectorIdPair != null;
        RelTraitSet traitSet = vector.getTraitSet().replace(DingoRelStreaming.of(vector.getTable()));

        // vector filter match primary point get
        if (prePrimaryOrScalarPlan(call, filter, vector, vectorIdPair, traitSet, selection)) {
            return;
        }

        // pre filtering
        // 1. range scan operator pushdown
        // plan A: DingoTableScan pushdown -> vector get distance and sorted by vectorIds
        // plan B: DingoTableScan pushdown -> vector search by vectorIds
        // 2. vector search operator pushdown
        // plan C:table_data pushdown operator -> vector get distance and sorted by vectorIds
        // plan D: table_data pushdown operator -> vector search by vectorIds
        // 3. scalar index -> vector search by vectorIds
        // scalar operator -> vector search by vectorIds
        // scalar operator -> vector get distance and sorted by vectorIds

        DingoTableScan dingoTableScan = new DingoTableScan(vector.getCluster(),
            traitSet,
            ImmutableList.of(),
            vector.getTable(),
            filter.getCondition(),
            selection,
            null,
            null,
            null,
            true
            );

        VectorStreamConvertor vectorStreamConvertor = new VectorStreamConvertor(
            vector.getCluster(),
            vector.getTraitSet(),
            dingoTableScan,
            vector.getIndexTableId(),
            vectorIdPair.getKey(),
            vector.getIndexTableDefinition(),
            false);
        DingoGetVectorByDistance dingoVectorGetDistance = new DingoGetVectorByDistance(
            vector.getCluster(),
            traitSet,
            vectorStreamConvertor,
            filter.getCondition(),
            vector.getTable(),
            vector.getOperands(),
            vectorIdPair.getKey(),
            vectorIdPair.getValue(),
            vector.getIndexTableId()
            );

        // pre filter
        call.transformTo(dingoVectorGetDistance);
    }

    private static DingoGetByIndex preScalarRelNode(DingoVector dingoVector,
                                         IndexValueMapSet<Integer, RexNode> indexValueMapSet,
                                         TableDefinition td,
                                         TupleMapping selection,
                                         DingoFilter filter) {
        Map<CommonId, TableDefinition> indexTdMap = getScalaIndices(dingoVector.getTable());

        if (indexTdMap.size() == 0) {
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

        return new DingoGetByIndex(
            dingoVector.getCluster(),
            dingoVector.getTraitSet(),
            ImmutableList.of(),
            dingoVector.getTable(),
            filter.getCondition(),
            selection,
            false,
            indexSetMap,
            indexTdMap
        );
    }

    private static boolean prePrimaryOrScalarPlan(RelOptRuleCall call,
                                          DingoFilter filter,
                                          DingoVector vector,
                                          Pair<Integer, Integer> vectorIdPair,
                                          RelTraitSet traitSet,
                                          TupleMapping selection) {
        DingoTable dingoTable = vector.getTable().unwrap(DingoTable.class);
        RexNode rexNode = RexUtil.toDnf(vector.getCluster().getRexBuilder(), filter.getCondition());
        IndexValueMapSetVisitor visitor = new IndexValueMapSetVisitor(vector.getCluster().getRexBuilder());
        IndexValueMapSet<Integer, RexNode> indexValueMapSet = rexNode.accept(visitor);
        assert dingoTable != null;
        final TableDefinition td = dingoTable.getTableDefinition();
        List<Integer> keyIndices = td.getKeyColumnIndices();

        Set<Map<Integer, RexNode>> keyMapSet = filterIndices(indexValueMapSet, keyIndices, selection);

        RelNode scan;
        if (keyMapSet != null) {
            scan = new DingoGetByKeys(
                vector.getCluster(),
                vector.getTraitSet(),
                ImmutableList.of(),
                vector.getTable(),
                filter.getCondition(),
                selection,
                keyMapSet
            );
        } else {
            scan = preScalarRelNode(vector, indexValueMapSet, td, selection, filter);
        }

        if (scan == null) {
            return false;
        }
        VectorStreamConvertor vectorStreamConvertor = new VectorStreamConvertor(
            vector.getCluster(),
            vector.getTraitSet(),
            scan,
            vector.getIndexTableId(),
            vectorIdPair.getKey(),
            vector.getIndexTableDefinition(),
            false);
        DingoGetVectorByDistance dingoVectorGetDistance = new DingoGetVectorByDistance(
            vector.getCluster(),
            traitSet,
            vectorStreamConvertor,
            filter.getCondition(),
            vector.getTable(),
            vector.getOperands(),
            vectorIdPair.getKey(),
            vectorIdPair.getValue(),
            vector.getIndexTableId()
        );
        RelTraitSet traits = vector.getCluster().traitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.ROOT);
        DingoStreamingConverter dingoStreamingConverterA = new DingoStreamingConverter(vector.getCluster(),
            traits, dingoVectorGetDistance);
        call.transformTo(dingoStreamingConverterA);
        return true;
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoVectorIndexRule.Config.builder()
            .description("DingoVectorIndexRule")
            .operandSupplier(b0 ->
                b0.operand(DingoFilter.class).oneInput(b1 ->
                    b1.operand(DingoVector.class).noInputs()
                )
            )
            .build();

        @Override
        default DingoVectorIndexRule toRule() {
            return new DingoVectorIndexRule(this);
        }
    }

    private static Pair<Integer, Integer> getVectorIndex(DingoTable dingoTable) {
        Map<CommonId, TableDefinition> indexDefinitions = dingoTable.getIndexTableDefinitions();
        for (Map.Entry<CommonId, TableDefinition> entry : indexDefinitions.entrySet()) {
            TableDefinition indexTableDefinition = entry.getValue();

            String indexType = indexTableDefinition.getProperties().get("indexType").toString();
            if (indexType.equals("scalar")) {
                continue;
            }

            String vectorIdColName = indexTableDefinition.getColumn(0).getName();
            String vectorColName = indexTableDefinition.getColumn(1).getName();
            int vectorIdIndex = 0;
            int vectorIndex = 0;
            for (int i = 0; i < dingoTable.getTableDefinition().getColumns().size(); i ++) {
                ColumnDefinition columnDefinition = dingoTable.getTableDefinition().getColumns().get(i);
                if (columnDefinition.getName().equals(vectorIdColName)) {
                    vectorIdIndex = i;
                } else if (columnDefinition.getName().equals(vectorColName)) {
                    vectorIndex = i;
                }
            }
            return Pair.of(vectorIdIndex, vectorIndex);
        }
        return null;
    }

    public static TupleMapping getDefaultSelection(DingoTable dingoTable) {
        int columnsCount = dingoTable.getTableDefinition().getColumnsCount();
        int[] mappings = new int[columnsCount];
        for (int i = 0; i < columnsCount; i ++) {
            mappings[i] = i;
        }
        return TupleMapping.of(mappings);
    }

}
