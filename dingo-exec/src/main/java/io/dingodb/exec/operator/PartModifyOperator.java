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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.Services;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.table.Part;
import io.dingodb.exec.table.PartInKvStore;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class PartModifyOperator extends SoleOutOperator {
    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId tableId;
    @JsonProperty("part")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId partId;
    @JsonProperty("schema")
    protected final DingoType schema;
    @JsonProperty("keyMapping")
    protected final TupleMapping keyMapping;

    protected Part part = null;
    protected long count;

    protected PartModifyOperator(
        CommonId tableId,
        CommonId partId,
        DingoType schema,
        TupleMapping keyMapping
    ) {
        super();
        this.tableId = tableId;
        this.partId = partId;
        this.schema = schema;
        this.keyMapping = keyMapping;
    }

    protected Part getPart() {
        return new PartInKvStore(
            Services.KV_STORE.getInstance(tableId, partId),
            CodecService.getDefault().createKeyValueCodec(tableId, schema, keyMapping)
        );
    }

    @Override
    public void init() {
        super.init();
        count = 0;
    }

    @Override
    public synchronized boolean push(int pin, @Nullable Object[] tuple) {
        if (part == null) {
            part = getPart();
        }
        return pushTuple(tuple);
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        if (!(fin instanceof FinWithException)) {
            output.push(new Object[]{count});
        }
        output.fin(fin);
        // Reset
        count = 0;
    }

    protected abstract boolean pushTuple(@Nullable Object[] tuple);
}
