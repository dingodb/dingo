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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.ExecutionContext;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolUtils;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.memory.MemoryController;
import io.dingodb.exec.memory.OperatorMemoryAllocatorCtx;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PROTECTED_AND_PUBLIC;

@Getter
@JsonAutoDetect(fieldVisibility = PROTECTED_AND_PUBLIC)
public abstract class PartModifyParam extends AbstractParams {

    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId tableId;
    @JsonProperty("schema")
    protected final DingoType schema;
    @JsonProperty("keyMapping")
    protected final TupleMapping keyMapping;
    @Setter
    protected long count;
    protected Table table;
    protected KeyValueCodec codec;
    @Setter
    protected NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions;

    @Getter
    protected AtomicLong memorySize;

    ExecutionContext executionContext;
    @Getter
    MemoryController memoryController;

    public PartModifyParam(
        CommonId tableId,
        DingoType schema,
        TupleMapping keyMapping,
        Table table,
        ExecutionContext executionContext
    ) {
        super();
        this.tableId = tableId;
        this.schema = schema;
        this.keyMapping = keyMapping;
        this.codec = CodecService.getDefault().createKeyValueCodec(
            table.getCodecVersion(), table.version, table.tupleType(), table.keyMapping());
        this.table = table;
        memorySize = new AtomicLong(0);
        this.executionContext = executionContext;
    }

    @Override
    public void init(Vertex vertex) {
        count = 0;
        if (!executionContext.isInnerSql()) {
            String name = "modify" + UUID.randomUUID();
            MemoryPool memoryPool =
                MemoryPoolUtils.createOperatorTmpTablePool(name, executionContext.getMemoryPool());
            memoryController = new MemoryController(memoryPool);
        }
    }

    public void reset() {
        count = 0;
        if (this.memoryController != null) {
            this.memoryController.close();
        }
    }
}
