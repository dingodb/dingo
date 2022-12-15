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
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.utils.QueueUtils;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
@JsonTypeName("root")
@JsonPropertyOrder({"schema"})
public final class RootOperator extends SinkOperator {
    public static final Object[] FIN = new Object[0];
    @JsonProperty("schema")
    private final DingoType schema;
    private Fin errorFin;
    private BlockingQueue<Object[]> tupleQueue;

    @JsonCreator
    public RootOperator(
        @JsonProperty("schema") DingoType schema
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
            log.debug("Put tuple {} into root queue.", schema.format(tuple));
        }
        QueueUtils.forcePut(tupleQueue, tuple);
        return true;
    }

    @Override
    public void fin(Fin fin) {
        if (fin instanceof FinWithException) {
            errorFin = fin;
            log.warn("Got FIN with exception: {}", fin.detail());
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Got FIN with detail:\n{}", fin.detail());
            }
        }
        QueueUtils.forcePut(tupleQueue, FIN);
    }

    public Object @NonNull [] popValue() {
        return QueueUtils.forceTake(tupleQueue);
    }

    public @NonNull TupleIterator getIterator() {
        return new TupleIterator();
    }

    public class TupleIterator implements Iterator<Object[]> {
        private Object[] current;

        private TupleIterator() {
            current = RootOperator.this.popValue();
        }

        @Override
        public boolean hasNext() {
            if (current != RootOperator.FIN) {
                return true;
            }
            if (errorFin != null) {
                String errorMsg = errorFin.detail();
                log.warn("Job [id = {}] has failed, ErrorMsg: {}", getTask().getJobId(), errorMsg);
                throw new IllegalStateException(errorMsg);
            }
            return false;
        }

        @Override
        public Object[] next() {
            Object[] result = current;
            current = RootOperator.this.popValue();
            return result;
        }
    }
}
