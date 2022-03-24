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
import io.dingodb.common.table.TableId;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.Services;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.table.Part;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.store.api.StoreInstance;

public abstract class PartModifyOperator extends SoleOutOperator {
    @JsonProperty("table")
    protected final TableId tableId;
    @JsonProperty("part")
    protected final Object partId;
    @JsonProperty("schema")
    protected final TupleSchema schema;
    @JsonProperty("keyMapping")
    protected final TupleMapping keyMapping;

    protected Part part;
    protected long count;

    protected PartModifyOperator(
        TableId tableId,
        Object partId,
        TupleSchema schema,
        TupleMapping keyMapping
    ) {
        super();
        this.tableId = tableId;
        this.partId = partId;
        this.schema = schema;
        this.keyMapping = keyMapping;
    }

    @Override
    public void init() {
        super.init();
        StoreInstance store = Services.KV_STORE.getInstance(task.getLocation().getPath());
        part = new PartInKvStore(
            store.getKvBlock(tableId, partId),
            schema,
            keyMapping
        );
        count = 0;
    }

    @Override
    public void fin(int pin, Fin fin) {
        output.push(new Object[]{count});
        output.fin(fin);
    }
}
