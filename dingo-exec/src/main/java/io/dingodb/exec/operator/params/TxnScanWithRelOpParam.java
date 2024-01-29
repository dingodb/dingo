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
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.expr.rel.RelOp;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

@Getter
@JsonTypeName("txnScanRel")
@JsonPropertyOrder({
    "tableId",
    "schema",
    "keyMapping",
    "outputSchema",
    "scanTs",
    "isolationLevel",
    "timeOut",
    "rel",
    "pushDown",
})
public class TxnScanWithRelOpParam extends ScanWithRelOpParam {
    @JsonProperty("isolationLevel")
    private final int isolationLevel;
    @JsonProperty("timeOut")
    private final long timeOut;
    @JsonProperty("scanTs")
    private long scanTs;

    public TxnScanWithRelOpParam(
        CommonId tableId,
        @NonNull DingoType schema,
        TupleMapping keyMapping,
        long scanTs,
        int isolationLevel,
        long timeOut,
        @NonNull RelOp relOp,
        DingoType outputSchema,
        boolean pushDown
    ) {
        super(tableId, schema, keyMapping, relOp, outputSchema, pushDown);
        this.scanTs = scanTs;
        this.isolationLevel = isolationLevel;
        this.timeOut = timeOut;
    }

    @Override
    public void setStartTs(long startTs) {
        super.setStartTs(startTs);
        this.scanTs = startTs;
    }
}
