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
import io.dingodb.calcite.rel.DingoGetDocumentByDistance;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.dag.Vertex;
// import io.dingodb.exec.operator.params.DocumentPointDistanceParam;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import lombok.AllArgsConstructor;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.function.Supplier;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.calcite.visitor.function.DingoDocumentVisitFun.getDocumentFloats;
import static io.dingodb.exec.utils.OperatorCodeUtils.VECTOR_POINT_DISTANCE;

public final class DingoGetDocumentByDistanceVisitFun {

    private DingoGetDocumentByDistanceVisitFun() {
    }

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        DingoGetDocumentByDistance rel
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);
        return DingoBridge.bridge(idGenerator, inputs, new OperatorSupplier(rel));
    }

    @AllArgsConstructor
    static class OperatorSupplier implements Supplier<Vertex> {

        final DingoGetDocumentByDistance rel;

        @Override
        public Vertex get() {
            DingoRelOptTable dingoRelOptTable = (DingoRelOptTable) rel.getTable();
            List<Float> targetDocument = getTargetDocument(rel.getOperands());
            IndexTable indexTable = getDocumentIndexTable(dingoRelOptTable, targetDocument.size());
            if (indexTable == null) {
                throw new RuntimeException("not found document index");
            }
            MetaService metaService = MetaService.root().getSubMetaService(dingoRelOptTable.getSchemaName());
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
                = metaService.getRangeDistribution(rel.getIndexTableId());

            int dimension = Integer.parseInt(indexTable.getProperties()
                .getOrDefault("dimension", targetDocument.size()).toString());
            String algType;
            if (indexTable.indexType == IndexType.VECTOR_FLAT) {
                algType = "FLAT";
            } else if (indexTable.indexType == IndexType.VECTOR_HNSW) {
                algType = "HNSW";
            } else {
                algType = "";
            }
            DocumentPointDistanceParam param = new DocumentPointDistanceParam(
                distributions.firstEntry().getValue(),
                rel.getDocumentIndex(),
                rel.getIndexTableId(),
                targetDocument,
                dimension,
                algType,
                indexTable.getProperties().getProperty("metricType"),
                rel.getSelection()
            );

            return new Vertex(VECTOR_POINT_DISTANCE, param);
        }
    }

    public static List<Float> getTargetDocument(List<Object> operandList) {
        Float[] document = getDocumentFloats(operandList);
        return Arrays.asList(document);
    }

    private static IndexTable getDocumentIndexTable(DingoRelOptTable dingoRelOptTable, int dimension) {
        DingoTable dingoTable = dingoRelOptTable.unwrap(DingoTable.class);
        List<IndexTable> indexes = dingoTable.getTable().getIndexes();
        for (IndexTable index : indexes) {
            if (!index.getIndexType().isDocument) {
                continue;
            }
            int dimension1 = Integer.parseInt(index.getProperties().getProperty("dimension"));
            if (dimension == dimension1) {
                return index;
            }
        }
        return null;
    }
}
