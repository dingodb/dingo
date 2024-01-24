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
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import lombok.Getter;

@Getter
@JsonTypeName("txnLikeScan")
@JsonPropertyOrder({
    "tableId", "part", "schema", "keyMapping", "filter", "selection", "prefix"
})
public class TxnLikeScanParam extends FilterProjectSourceParam {

    @JsonProperty("prefix")
    private final byte[] prefix;
    private transient KeyValueCodec codec;

    public TxnLikeScanParam(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") CommonId partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("filter") SqlExpr filter,
        @JsonProperty("selection") TupleMapping selection,
        @JsonProperty("prefix") byte[] prefix
    ) {
        super(tableId, partId, schema, filter, selection, keyMapping);
        this.prefix = prefix;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        this.codec = CodecService.getDefault().createKeyValueCodec(schema, keyMapping);
    }
}
