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
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.util.QueueUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import javax.annotation.Nonnull;

@Slf4j
@JsonTypeName("root")
@JsonPropertyOrder({"schema"})
public final class RootOperator extends SinkOperator {
    public static final Object[] FIN = new Object[0];

    @Getter
    private Fin errorFin;

    @JsonProperty("schema")
    private final TupleSchema schema;

    private BlockingQueue<Object[]> tupleQueue;

    @JsonCreator
    public RootOperator(
        @JsonProperty("schema") TupleSchema schema
    ) {
        super();
        this.schema = schema;
    }

    @Override
    public void init() {
        super.init();
        tupleQueue = new LinkedBlockingDeque<>();
    }

    @Override
    public boolean push(Object[] tuple) {
        if (log.isDebugEnabled()) {
            log.debug("Put tuple {} into root queue.", schema.formatTuple(tuple));
        }
        QueueUtil.forcePut(tupleQueue, tuple);
        return true;
    }

    @Override
    public void fin(Fin fin) {
        if (log.isDebugEnabled()) {
            log.debug("Received FIN. {}", fin.detail());
        }

        if (fin instanceof FinWithException) {
            errorFin = fin;
            log.warn("Receive FinWith Exception:{}", fin.detail());
        }
        QueueUtil.forcePut(tupleQueue, FIN);
    }

    @Nonnull
    public Object[] popValue() {
        return QueueUtil.forceTake(tupleQueue);
    }

}
