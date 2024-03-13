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
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.utils.QueueUtils;
import lombok.Getter;
import lombok.Setter;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Getter
@JsonTypeName("root")
@JsonPropertyOrder({"schema", "takeTtl"})
public class RootParam extends AbstractParams {

    public static final int TUPLE_QUEUE_SIZE = 512;

    @JsonProperty("schema")
    private final DingoType schema;
    @JsonProperty("selection")
    private final @Nullable TupleMapping selection;
    @Setter
    private transient Fin errorFin;
    private transient BlockingQueue<Object[]> tupleQueue;
    @Setter
    private transient long takeTtl;

    @Setter
    private transient Long autoIncId;

    public RootParam(DingoType schema, @Nullable TupleMapping selection) {
        this.schema = schema;
        this.selection = selection;
    }

    @Override
    public void init(Vertex vertex) {
        tupleQueue = new LinkedBlockingDeque<>(TUPLE_QUEUE_SIZE);
    }

    public void forcePut(Object[] tuple) {
        QueueUtils.forcePut(tupleQueue, tuple);
    }

    public Object[] forceTake() {
        if (takeTtl == 0) {
            return QueueUtils.forceTake(tupleQueue);
        } else {
            return QueueUtils.forceTake(tupleQueue, takeTtl);
        }
    }
}
