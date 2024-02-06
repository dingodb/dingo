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
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.meta.entity.Table;
import lombok.Getter;

@Getter
@JsonTypeName("txnGet")
@JsonPropertyOrder({"scanTs", "isolationLevel", "timeOut", "isSelect", "table",
    "part", "schema", "keyMapping", "keys", "filter", "selection"})
public class TxnGetByKeysParam extends FilterProjectParam {

    private KeyValueCodec codec;
    private Table table;
    @JsonProperty("scanTs")
    private long scanTs;
    @JsonProperty("isolationLevel")
    private final int isolationLevel;
    @JsonProperty("timeOut")
    private final long timeOut;
    @JsonProperty("isSelect")
    private final boolean isSelect;

    public TxnGetByKeysParam(
        CommonId tableId,
        DingoType schema,
        TupleMapping keyMapping,
        SqlExpr filter,
        TupleMapping selection,
        Table table,
        long scanTs,
        int isolationLevel,
        long timeOut,
        boolean isSelect
    ) {
        super(tableId, schema, filter, selection, keyMapping);
        this.codec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());
        this.table = table;
        this.scanTs = scanTs;
        this.isolationLevel = isolationLevel;
        this.timeOut = timeOut;
        this.isSelect = isSelect;
    }

    @Override
    public void setStartTs(long startTs) {
        this.scanTs = startTs;
    }
}
