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
import io.dingodb.meta.entity.Table;
import lombok.Getter;

@Getter
@JsonTypeName("forUpdate")
@JsonPropertyOrder({"table", "primaryLockKey", "schema", "startTs",
    "forUpdateTs", "isolationLevel", "lockTimeOut", "isScan"})
public class ForUpdateParam extends AbstractParams {

    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId tableId;
    @JsonProperty("primaryLockKey")
    private final byte[] primaryLockKey;
    @JsonProperty("schema")
    private DingoType schema;
    @JsonProperty("startTs")
    private long startTs;
    @JsonProperty("forUpdateTs")
    private long forUpdateTs;
    @JsonProperty("isolationLevel")
    private int isolationLevel;
    @JsonProperty("lockTimeOut")
    private long lockTimeOut;
    @JsonProperty("isScan")
    private boolean isScan;
    private Table table;
    private KeyValueCodec codec;

    public ForUpdateParam(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("primaryLockKey") byte[] primaryLockKey,
        @JsonProperty("startTs") long startTs,
        @JsonProperty("forUpdateTs") long forUpdateTs,
        @JsonProperty("isolationLevel") int isolationLevel,
        @JsonProperty("lockTimeOut") long lockTimeOut,
        @JsonProperty("isScan") boolean isScan,
        Table table
    ) {
        super();
        this.tableId = tableId;
        this.schema = schema;
        this.primaryLockKey = primaryLockKey;
        this.startTs = startTs;
        this.forUpdateTs = forUpdateTs;
        this.isolationLevel = isolationLevel;
        this.lockTimeOut = lockTimeOut;
        this.isScan = isScan;
        this.table = table;
        this.codec = CodecService.getDefault().createKeyValueCodec(
            table.getCodecVersion(), table.version, table.tupleType(), table.keyMapping()
        );
    }
}
