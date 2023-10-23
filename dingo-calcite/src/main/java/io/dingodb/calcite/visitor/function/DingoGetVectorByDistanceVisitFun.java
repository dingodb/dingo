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

package io.dingodb.calcite.visitor.function;

import io.dingodb.calcite.DingoRelOptTable;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoGetVectorByDistance;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.operator.VectorPointDistanceOperator;
import io.dingodb.meta.MetaService;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlNode;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.function.Supplier;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.calcite.visitor.function.DingoVectorVisitFun.getVectorFloats;

public final class DingoGetVectorByDistanceVisitFun {

    public static Collection<Output> visit(
        Job job, IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        DingoGetVectorByDistance rel
    ) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(visitor);
        return DingoBridge.bridge(idGenerator, inputs, new OperatorSupplier(rel));
    }

    @AllArgsConstructor
    static class OperatorSupplier implements Supplier<Operator> {

        final DingoGetVectorByDistance rel;

        @Override
        public Operator get() {
            DingoRelOptTable dingoRelOptTable = (DingoRelOptTable) rel.getTable();
            List<Float> targetVector = getTargetVector(rel.getOperands());
            Properties properties = getVectorProperties(dingoRelOptTable, targetVector.size());
            if (properties == null) {
                throw new RuntimeException("not found vector index");
            }
            MetaService metaService = MetaService.root().getSubMetaService(dingoRelOptTable.getSchemaName());
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
                = metaService.getIndexRangeDistribution(rel.getIndexTableId());

            int dimension = Integer.parseInt(properties.getOrDefault("dimension", targetVector.size()).toString());
            VectorPointDistanceOperator operator = new VectorPointDistanceOperator(
                distributions.firstEntry().getValue(),
                rel.getVectorIndex(),
                rel.getIndexTableId(),
                targetVector,
                dimension,
                properties.getProperty("type"),
                properties.getProperty("metricType"));
            return operator;
        }
    }

    public static List<Float> getTargetVector(List<SqlNode> operandList) {
        Float[] vector = getVectorFloats(operandList);
        return Arrays.asList(vector);
    }

    private static Properties getVectorProperties(DingoRelOptTable dingoRelOptTable, int dimension) {
        DingoTable dingoTable = dingoRelOptTable.unwrap(DingoTable.class);
        Map<CommonId, TableDefinition> indexDefinitions = dingoTable.getIndexTableDefinitions();
        for (Map.Entry<CommonId, TableDefinition> entry : indexDefinitions.entrySet()) {
            TableDefinition indexTableDefinition = entry.getValue();

            String indexType = indexTableDefinition.getProperties().get("indexType").toString();
            if (indexType.equals("scalar")) {
                continue;
            }
            int dimension1 = Integer.parseInt(indexTableDefinition.getProperties().getProperty("dimension"));
            if (dimension == dimension1) {
                return indexTableDefinition.getProperties();
            }
        }
        return null;
    }
}
