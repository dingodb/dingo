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
import io.dingodb.calcite.rel.DingoGetDocumentPreFilter;
import io.dingodb.calcite.utils.VisitUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.DocumentPreFilterParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.function.Supplier;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.calcite.visitor.function.DingoDocumentVisitFun.getDocuementIndexName;
import static io.dingodb.calcite.visitor.function.DingoDocumentVisitFun.getDocumentQueryStr;
import static io.dingodb.calcite.visitor.function.DingoDocumentVisitFun.getDocumentTopK;
import static io.dingodb.calcite.visitor.function.DingoDocumentVisitFun.getDocuementTabName;
import static io.dingodb.exec.utils.OperatorCodeUtils.DOCUMENT_PRE_FILTER;

public final class DingoGetDocumentPreFilterVisitFun {

    private DingoGetDocumentPreFilterVisitFun() {
    }

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator,
        Location currentLocation,
        ITransaction transaction,
        DingoJobVisitor visitor,
        @NonNull DingoGetDocumentPreFilter rel
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);
        return DingoBridge.bridge(idGenerator, inputs, new OperatorSupplier(rel, transaction, visitor));
    }

    @AllArgsConstructor
    static class OperatorSupplier implements Supplier<Vertex> {

        final DingoGetDocumentPreFilter rel;
        final ITransaction transaction;
        final DingoJobVisitor visitor;

        @Override
        public Vertex get() {
            DingoRelOptTable dingoRelOptTable = (DingoRelOptTable) rel.getTable();
            String queryString = getQueryString(rel.getOperands());
            String tabName = getTableName(rel.getOperands());
            Integer topK = getTopk(rel.getOperands());
            Table table = getTable(dingoRelOptTable);
            String indexName = getIndexName(rel.getOperands());
            IndexTable indexTable = getDocumentIndexTable(dingoRelOptTable);
            List<Column> columnNames = indexTable.getColumns();
            for (Column columnName : columnNames) {
                queryString = queryString.replaceAll(
                    columnName.getName().toLowerCase() + ":",
                    columnName.getName().toUpperCase() + ":"
                );
            }
            if (indexTable == null) {
                throw new RuntimeException("not found document index");
            }
            long scanTs = VisitUtils.getScanTs(transaction, visitor.getKind());
            MetaService metaService = MetaService.root().getSubMetaService(dingoRelOptTable.getSchemaName());
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
                = metaService.getRangeDistribution(rel.getIndexTableId());

            DocumentPreFilterParam param = new DocumentPreFilterParam(
                distributions,
                table,
                rel.getDocumentPriIdIndex(),
                rel.getIndexTableId(),
                queryString,
                rel.getSelection(),
                tabName,
                indexName,
                scanTs,
                topK
            );

            return new Vertex(DOCUMENT_PRE_FILTER, param);
        }
    }

    public static String getQueryString(List<Object> operandList) {
        String queryStr = getDocumentQueryStr(operandList);
        return queryStr;
    }

    public static String getTableName(List<Object> operandList) {
        String tabName = getDocuementTabName(operandList);
        return tabName;
    }

    public static String getIndexName(List<Object> operandList) {
        String tabName = getDocuementIndexName(operandList);
        return tabName;
    }

    public static Integer getTopk(List<Object> operandList) {
        Integer topk = getDocumentTopK(operandList).intValue();
        return topk;
    }
    public static Table getTable(DingoRelOptTable dingoRelOptTable){
        DingoTable dingoTable = dingoRelOptTable.unwrap(DingoTable.class);
        return dingoTable.getTable();
    }
    private static IndexTable getDocumentIndexTable(DingoRelOptTable dingoRelOptTable) {
        DingoTable dingoTable = dingoRelOptTable.unwrap(DingoTable.class);
        List<IndexTable> indexes = dingoTable.getTable().getIndexes();
        for (IndexTable index : indexes) {
            if (index.getIndexType() != IndexType.DOCUMENT) {
                continue;
            }
                return index;
        }
        return null;
    }
}
