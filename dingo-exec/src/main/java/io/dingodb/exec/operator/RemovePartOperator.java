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
import io.dingodb.common.CommonId;
import io.dingodb.common.table.Part;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.Services;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@JsonTypeName("removePart")
@JsonPropertyOrder({"table", "partStartKey", "schema", "keyMapping", "output"})
public final class RemovePartOperator extends SourceOperator {
    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId tableId;

    @JsonProperty("startKeyList")
    protected final List<String> startKeyLists;

    @JsonProperty("schema")
    protected final TupleSchema schema;

    @JsonProperty("keyMapping")
    protected final TupleMapping keyMapping;

    protected Part part;

    @JsonCreator
    public RemovePartOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("startKeyList")  List<String> partStartKey,
        @JsonProperty("schema") TupleSchema schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping
    ) {
        this.tableId = tableId;
        this.startKeyLists = partStartKey;
        this.keyMapping = keyMapping;
        this.schema = schema;
    }

    @Override
    public void init() {
        super.init();
        StoreInstance store = Services.KV_STORE.getInstance(tableId);
        part = new PartInKvStore(
            store,
            schema,
            keyMapping
        );
    }

    @Override
    public boolean push() {
        OperatorProfile profile = getProfile();
        profile.setStartTimeStamp(System.currentTimeMillis());
        final long startTime = System.currentTimeMillis();
        long count = part.getEntryCntAndDeleteByPart(startKeyLists);
        output.push(new Object[] {count});
        if (log.isDebugEnabled()) {
            log.debug("delete table by partition, get count: {}, cost: {} ms.",
                count, System.currentTimeMillis() - startTime);
        }
        profile.setProcessedTupleCount(count);
        profile.setEndTimeStamp(System.currentTimeMillis());
        return false;
    }
}
