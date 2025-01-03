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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import lombok.Getter;

import java.util.NavigableMap;

@Getter
@JsonTypeName("txn_diskAnnLoad")
@JsonPropertyOrder({
    "tableId", "part", "schema", "keyMapping", "filter",
    "selection", "indexId", "indexRegionId", "nodesCacheNum", "warmup"
})
public class TxnDiskAnnLoadParam extends FilterProjectSourceParam {
    private KeyValueCodec codec;
    private final Table table;

    private final NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions;
    private final CommonId indexId;
    private final IndexTable indexTable;
    @JsonProperty("scanTs")
    private long scanTs;
    @JsonProperty("filter")
    protected SqlExpr filter;
    @JsonProperty("isolationLevel")
    private final int isolationLevel;
    @JsonProperty("timeOut")
    private final long timeOut;
    @JsonProperty("selection")
    protected TupleMapping selection;
    @JsonProperty("nodesCacheNum")
    private final int nodesCacheNum;
    @JsonProperty("warmup")
    private final boolean warmup;

    public TxnDiskAnnLoadParam(
        CommonId partId,
        SqlExpr filter,
        TupleMapping selection,
        DingoType schema,
        Table table,
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions,
        Table indexTable,
        long scanTs,
        int isolationLevel,
        long timeOut,
        int nodesCacheNum,
        boolean warmup
    ) {
        super(table.tableId, partId, schema, table.version, filter, selection, table.keyMapping());
        this.table = table;
        this.distributions = distributions;
        this.indexId = indexTable.tableId;
        this.indexTable = (IndexTable) indexTable;
        this.scanTs = scanTs;
        this.isolationLevel = isolationLevel;
        this.timeOut = timeOut;
        this.nodesCacheNum = nodesCacheNum;
        this.warmup = warmup;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        codec = CodecService.getDefault().createKeyValueCodec(
            table.getCodecVersion(), schemaVersion, schema, keyMapping);
    }

    @Override
    public void setStartTs(long startTs) {
        this.scanTs = startTs;
    }

}
