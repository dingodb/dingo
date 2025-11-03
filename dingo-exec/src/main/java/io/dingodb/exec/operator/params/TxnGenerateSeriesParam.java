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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.expr.common.type.IntervalType;
import io.dingodb.meta.entity.Table;
import lombok.Getter;

import java.math.BigDecimal;

@Getter
@JsonTypeName("txn_generate_series")
@JsonPropertyOrder({ "tableId", "startCol", "endCol", "step" })
public class TxnGenerateSeriesParam extends FilterProjectSourceParam {

    private KeyValueCodec codec;
    private final Table table;
    private final RangeDistribution rangeDistribution;

    private final Object startCol;
    private final Object endCol;
    private final Object step;
    private final TupleMapping mapping;

    private final long timeout;
    private final long scanTs;

    public TxnGenerateSeriesParam(
        CommonId partId,
        DingoType schema,
        SqlExpr filter,
        Table table,
        TupleMapping selection,
        Object startCol,
        Object endCol,
        Object step,
        TupleMapping mapping,
        RangeDistribution rangeDistribution,
        long timeout,
        long scanTs) {
        super(table.tableId, partId, schema, table.version, filter, selection, table.keyMapping());
        this.rangeDistribution = rangeDistribution;
        this.table = table;
        this.startCol = startCol;
        this.endCol = endCol;
        this.step = step;
        this.mapping = mapping;
        this.timeout = timeout;
        this.scanTs = scanTs;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        codec = CodecService.getDefault().createKeyValueCodec(
            table.getCodecVersion(), schemaVersion, schema, keyMapping
        );
    }

    public BigDecimal getBigDecimalStep() {
        if (step instanceof BigDecimal) {
            return (BigDecimal) step;
        }
        return null;
    }

    public IntervalType getIntervalStep() {
        if (step instanceof IntervalType) {
            return (IntervalType) step;
        }
        return null;
    }
}
