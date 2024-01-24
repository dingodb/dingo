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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import lombok.Getter;

@Getter
@JsonTypeName("txnRangeDelete")
@JsonPropertyOrder({
    "table", "part", "schema", "keyMapping", "filter", "selection"})
public class TxnPartRangeDeleteParam extends AbstractParams {

    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId tableId;
    @JsonProperty("schema")
    private final DingoType schema;
    @JsonProperty("keyMapping")
    private final TupleMapping keyMapping;

    private transient KeyValueCodec codec;

    public TxnPartRangeDeleteParam(
        CommonId tableId,
        DingoType schema,
        TupleMapping keyMapping
    ) {
        super();
        this.tableId = tableId;
        this.schema = schema;
        this.keyMapping = keyMapping;
    }

    @Override
    public void init(Vertex vertex) {
        this.codec = CodecService.getDefault().createKeyValueCodec(schema, keyMapping);
    }
}
