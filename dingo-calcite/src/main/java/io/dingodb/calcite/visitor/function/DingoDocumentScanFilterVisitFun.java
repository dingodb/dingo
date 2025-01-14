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

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.dingo.DingoDocumentScanFilter;
import io.dingodb.calcite.utils.MetaServiceUtils;
import io.dingodb.calcite.utils.TableInfo;
import io.dingodb.calcite.utils.VisitUtils;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.TxnDocumentScanFilterParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.exec.utils.OperatorCodeUtils.DOCUMENT_SCAN_FILTER;

public final class DingoDocumentScanFilterVisitFun {
    private DingoDocumentScanFilterVisitFun() {
    }

    public static @NonNull Collection<Vertex> visit(
        Job job,
        @NonNull IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        ITransaction transaction,
        @NonNull DingoDocumentScanFilter rel
    ) {
        final LinkedList<Vertex> outputs = new LinkedList<>();
        MetaService metaService = MetaService.root(visitor.getPointTs());
        final Table td = Objects.requireNonNull(rel.getTable().unwrap(DingoTable.class)).getTable();
        CommonId idxId = rel.getIndexId();
        Table indexTd = rel.getIndexTable();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> indexRanges = metaService
            .getRangeDistribution(idxId);

        List<Column> columnNames = indexTd.getColumns();
        List<Integer> indexSelectionList = columnNames.stream().map(td.columns::indexOf).collect(Collectors.toList());
        TableInfo tableInfo = MetaServiceUtils.getTableInfo(visitor.getPointTs(), rel.getTable());
        TupleMapping tupleMapping = TupleMapping.of(
            indexSelectionList
        );
        long scanTs = VisitUtils.getScanTs(transaction, visitor.getKind(), visitor.getPointTs());

        for (RangeDistribution rangeDistribution : indexRanges.values()) {
            TxnDocumentScanFilterParam param = new TxnDocumentScanFilterParam(
                rangeDistribution.id(),
                idxId,
                tableInfo.getId(),
                tupleMapping,
                null,
                rel.getSelection(),
                indexTd,
                td,
                rel.isLookup(),
                scanTs,
                transaction.getLockTimeOut(),
                rel.getQueryString()
            );
            Vertex  vertex = new Vertex(DOCUMENT_SCAN_FILTER, param);
            Task task = job.getOrCreate(currentLocation, idGenerator);
            OutputHint hint = new OutputHint();
            hint.setPartId(rangeDistribution.id());
            vertex.setHint(hint);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            outputs.add(vertex);
        }

        visitor.setScan(true);
        return outputs;
    }
}
