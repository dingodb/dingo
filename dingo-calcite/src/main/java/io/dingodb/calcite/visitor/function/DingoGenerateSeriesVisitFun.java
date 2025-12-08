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
import io.dingodb.calcite.rel.DingoGenerateSeries;
import io.dingodb.calcite.type.DingoIntervalLiteral;
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
import io.dingodb.exec.operator.params.TxnGenerateSeriesParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.expr.common.type.IntervalDayType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_GENERATE_SERIES;

@Slf4j
public class DingoGenerateSeriesVisitFun {

    private DingoGenerateSeriesVisitFun() {

    }

    public static Collection<Vertex> visit(
        Job job, IdGenerator idGenerator, Location currentLocation,
        ITransaction transaction, DingoJobVisitor visitor, DingoGenerateSeries rel) {
        if (transaction == null) {
            throw new RuntimeException("not support Non-transaction");
        }
        DingoRelOptTable relTable = rel.getTable();
        DingoTable dingoTable = relTable.unwrap(DingoTable.class);

        MetaService metaService = MetaService.root(visitor.getPointTs());
        assert dingoTable != null;

        CommonId tableId = dingoTable.getTableId();
        Table td = dingoTable.getTable();

        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
            metaService.getRangeDistribution(tableId);
        List<Object> operands = rel.getOperands();

        Column startColumn = null;
        Column endColumn = null;
        if (operands.get(1) instanceof SqlIdentifier && operands.get(2) instanceof SqlIdentifier) {
            SqlIdentifier arg1 = (SqlIdentifier) operands.get(1);
            String startCol = arg1.getLastName();
            Column c1 = td.getColumn(startCol);
            if (c1 == null) {
                throw new RuntimeException("Column " + startCol + " does not exist in table " + td.getName());
            }
            startColumn = c1;

            SqlIdentifier arg2 = (SqlIdentifier) operands.get(2);
            String endCol = arg2.getLastName();
            Column c2 = td.getColumn(endCol);
            if (c2 == null) {
                throw new RuntimeException("Column " + endCol + " does not exist in table " + td.getName());
            }
            endColumn = c2;
        }
        Object step = 1;
        DingoIntervalLiteral intervalLiteral = null;
        if (operands.get(3) instanceof DingoIntervalLiteral) {
            intervalLiteral = (DingoIntervalLiteral) operands.get(3);
        }
        if (intervalLiteral == null) {
            if (operands.get(3) instanceof SqlNumericLiteral) {
                step = ((SqlNumericLiteral) operands.get(3)).getValueAs(BigDecimal.class);
            }
        }
        long scanTs = VisitUtils.getScanTs(transaction, visitor.getKind(), visitor.getPointTs(), visitor.isForUpdate());
        if (intervalLiteral != null) {
            SqlIntervalLiteral.IntervalValue value = (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
            SqlIntervalQualifier qualifier = value.getIntervalQualifier();
            String literal = value.getIntervalLiteral();
            if (qualifier.getStartUnit() == TimeUnit.MONTH) {
                step = new IntervalMonthType.IntervalMonth(BigDecimal.valueOf(Long.parseLong(literal)), null);
            } else if (qualifier.getStartUnit() == TimeUnit.DAY) {
                step = new IntervalDayType.IntervalDay(BigDecimal.valueOf(Long.parseLong(literal)), null);
            } else {
                throw new UnsupportedOperationException(qualifier.getStartUnit().name());
            }
        }
        TupleMapping mapping = null;
        if (operands.size() > 4 && operands.get(4) instanceof SqlBasicCall) {
            List<String> columnNames = new ArrayList<>();
            SqlBasicCall call = (SqlBasicCall) operands.get(4);
            List<SqlNode> operandList = call.getOperandList();
            for (SqlNode node : operandList) {
                if (node instanceof SqlIdentifier) {
                    SqlIdentifier identifier = (SqlIdentifier) node;
                    columnNames.add(identifier.getLastName());
                }
            }
            if (!columnNames.isEmpty()) {
                mapping = TupleMapping.of(td.getColumnIndices(columnNames));
            }
        } else if (operands.size() > 4 && operands.get(4) instanceof SqlIdentifier
            && ((SqlIdentifier) operands.get(4)).isStar()) {
            mapping = td.mapping();
        } else if (operands.size() > 4 && operands.get(4) instanceof SqlIdentifier
            && !((SqlIdentifier) operands.get(4)).isStar()) {
            SqlIdentifier identifier = (SqlIdentifier) operands.get(4);
            mapping = TupleMapping.of(new int[]{td.getColumnIndex(identifier.getLastName())});
        }

        List<Vertex> outputs = new ArrayList<>();

        for (RangeDistribution range : ranges.values()) {
            TxnGenerateSeriesParam param = new TxnGenerateSeriesParam(
                range.id(),
                td.tupleType(),
                null,
                td,
                rel.getSelection(),
                startColumn,
                endColumn,
                step,
                mapping,
                range,
                transaction.getLockTimeOut(),
                scanTs);
            Vertex vertex = new Vertex(TXN_GENERATE_SERIES, param);
            Task task = job.getOrCreate(currentLocation, idGenerator);
            OutputHint hint = new OutputHint();
            hint.setPartId(range.id());
            vertex.setHint(hint);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            outputs.add(vertex);
        }
        if (outputs.isEmpty()) {
            throw new RuntimeException("The view does not support the generate_series function");
        }
        return outputs;
    }
}
