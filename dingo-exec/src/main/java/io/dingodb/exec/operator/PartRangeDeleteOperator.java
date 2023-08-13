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

package io.dingodb.exec.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.Services;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.table.Part;
import io.dingodb.exec.table.PartInKvStore;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@JsonTypeName("rangeDelete")
@JsonPropertyOrder({
    "table", "part", "schema", "keyMapping", "filter", "selection", "output",
    "startKey", "endKey", "includeStart", "includeEnd"})
public final class PartRangeDeleteOperator extends SourceOperator {
    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId tableId;

    @JsonProperty("part")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId partId;

    @JsonProperty("schema")
    private final DingoType schema;
    @JsonProperty("keyMapping")
    private final TupleMapping keyMapping;

    @JsonProperty("startKey")
    private final byte[] startKey;
    @JsonProperty("endKey")
    private final byte[] endKey;

    @JsonProperty("includeStart")
    private final boolean includeStart;
    @JsonProperty("includeEnd")
    private final boolean includeEnd;

    private Part part;

    @JsonCreator
    public PartRangeDeleteOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") CommonId partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("startKey") byte[] startKey,
        @JsonProperty("endKey") byte[] endKey,
        @JsonProperty("includeStart") boolean includeStart,
        @JsonProperty("includeEnd") boolean includeEnd
    ) {
        this.tableId = tableId;
        this.partId = partId;
        this.schema = schema;
        this.keyMapping = keyMapping;
        this.startKey = startKey;
        this.endKey = endKey;
        this.includeStart = includeStart;
        this.includeEnd = includeEnd;
    }

    @Override
    public void init() {
        super.init();
        part = new PartInKvStore(
            Services.KV_STORE.getInstance(tableId, partId),
            CodecService.getDefault().createKeyValueCodec(schema, keyMapping)
        );
    }

    @Override
    public boolean push() {
        OperatorProfile profile = getProfile();
        profile.setStartTimeStamp(System.currentTimeMillis());
        final long startTime = System.currentTimeMillis();
        long count = part.delete(startKey, endKey, includeStart, includeEnd);
        output.push(new Object[]{count});
        if (log.isDebugEnabled()) {
            log.debug("Delete data by range, delete count: {}, cost: {} ms.",
                count, System.currentTimeMillis() - startTime);
        }
        profile.setProcessedTupleCount(count);
        profile.setEndTimeStamp(System.currentTimeMillis());
        return false;
    }
}
