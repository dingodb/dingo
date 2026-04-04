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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryManager;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolUtils;
import io.dingodb.common.memory.QueryMemoryPool;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.aggregate.AbstractAgg;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.aggregate.AggCache;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.memory.OperatorMemoryAllocatorCtx;
import io.dingodb.exec.operator.spill.SpillManager;
import io.dingodb.exec.operator.spill.TupleSpillFile;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

@Slf4j
@JsonTypeName("aggregate")
@JsonPropertyOrder({"keys", "aggregates", "schema", "spillThreshold"})
public class AggregateParams extends AbstractParams implements RevokerParams {

    @JsonProperty("keys")
    private final TupleMapping keyMapping;

    @JsonProperty("aggregates")
    @JsonSerialize(contentAs = AbstractAgg.class)
    @JsonDeserialize(contentAs = AbstractAgg.class)
    private final List<Agg> aggList;

    /**
     * Optional input row schema used to serialize spill data.
     * When {@code null}, spill-to-disk is disabled.
     */
    @JsonProperty("schema")
    private final DingoType schema;

    /**
     * Maximum number of input tuples buffered in memory before spilling to disk.
     * A value of {@code 0} means "use the default from {@link SpillManager}".
     */
    @JsonProperty("spillThreshold")
    private final int spillThreshold;

    @Getter
    private transient AggCache cache;
    /** Input row buffer used when spill is enabled. */
    private transient List<Object[]> inputBuffer;
    /** Spill files for input runs written to disk; {@code null} when spill is disabled. */
    private transient List<TupleSpillFile> spillFiles;
    /** Total number of input rows already written to spill files. */
    private transient long spilledCount;
    /** Per-operator query-level memory pool (for scheduler integration). */
    private transient QueryMemoryPool queryMemoryPool;
    /** Memory allocator context used by the memory-revoking scheduler. */
    private transient OperatorMemoryAllocatorCtx memoryAllocatorCtx;

    @JsonCreator
    public AggregateParams(
        @JsonProperty("keys") TupleMapping keyMapping,
        @JsonProperty("aggregates") List<Agg> aggList,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("spillThreshold") int spillThreshold
    ) {
        this.keyMapping = keyMapping;
        this.aggList = aggList;
        this.schema = schema;
        this.spillThreshold = spillThreshold;
    }

    /** Convenience constructor for callers that do not need spill support (backward compat). */
    public AggregateParams(TupleMapping keyMapping, List<Agg> aggList) {
        this(keyMapping, aggList, null, 0);
    }

    @Override
    public void init(Vertex vertex) {
        cache = new AggCache(keyMapping, aggList);
        if (schema != null) {
            inputBuffer = new ArrayList<>();
            spillFiles = new ArrayList<>();
            spilledCount = 0;
            if (ScopeVariables.enableSpill()) {
                String poolName = "aggregate-" + UUID.randomUUID();
                queryMemoryPool = (QueryMemoryPool) MemoryManager.getInstance()
                    .createQueryMemoryPool(false, poolName);
                MemoryPool opPool = MemoryPoolUtils.createOperatorTmpTablePool(
                    poolName + "-op", queryMemoryPool);
                memoryAllocatorCtx = new OperatorMemoryAllocatorCtx(opPool, true);
            }
        }
    }

    /**
     * Adds an input tuple. When spill is enabled, tuples are buffered and spilled to disk
     * when the buffer reaches the configured threshold or when the memory-revoking scheduler
     * requests it. Otherwise, tuples are aggregated directly into the in-memory {@link AggCache}.
     */
    public synchronized void addTuple(Object[] tuple) {
        if (schema != null) {
            inputBuffer.add(tuple);
            boolean shouldSpill = inputBuffer.size() >= getEffectiveSpillThreshold()
                || (memoryAllocatorCtx != null && memoryAllocatorCtx.isMemoryRevokingRequested());
            if (shouldSpill) {
                try {
                    spillCurrentBuffer();
                    if (memoryAllocatorCtx != null) {
                        memoryAllocatorCtx.releaseRevocableMemory(
                            memoryAllocatorCtx.getRevocableAllocated(), true);
                        memoryAllocatorCtx.resetMemoryRevokingRequested();
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to spill aggregate input buffer to disk", e);
                }
            }
        } else {
            cache.addTuple(tuple);
        }
    }

    /**
     * Aggregates all buffered and spilled input tuples into the {@link AggCache}, making
     * results available via {@link #getCache()}. This is a no-op when spill is disabled.
     *
     * @throws IOException if a spill file cannot be read
     */
    public void prepareResults() throws IOException {
        if (schema == null) {
            return;
        }
        // Aggregate remaining in-memory buffer
        for (Object[] tuple : inputBuffer) {
            cache.addTuple(tuple);
        }
        inputBuffer.clear();
        // Aggregate all spilled input tuples, closing each file immediately after reading
        for (TupleSpillFile sf : spillFiles) {
            try {
                Iterator<Object[]> it = sf.iterator();
                while (it.hasNext()) {
                    cache.addTuple(it.next());
                }
            } finally {
                sf.close();
            }
        }
        spillFiles.clear();
    }

    /** Returns the effective spill threshold (always positive). */
    public int getEffectiveSpillThreshold() {
        return spillThreshold > 0 ? spillThreshold : SpillManager.DEFAULT_SPILL_THRESHOLD;
    }

    /** Returns whether spill-to-disk is enabled for this parameter set. */
    public boolean isSpillEnabled() {
        return schema != null;
    }

    public void clear() {
        cache.clear();
        spilledCount = 0;
        if (inputBuffer != null) {
            inputBuffer.clear();
        }
        if (spillFiles != null) {
            for (TupleSpillFile sf : spillFiles) {
                sf.close();
            }
            spillFiles.clear();
        }
        if (memoryAllocatorCtx != null) {
            memoryAllocatorCtx.releaseRevocableMemory(memoryAllocatorCtx.getRevocableAllocated(), true);
        }
        if (queryMemoryPool != null) {
            queryMemoryPool.destroy();
            queryMemoryPool = null;
        }
    }

    @Override
    public MemoryPool getQueryMemoryPool() {
        return queryMemoryPool;
    }

    @Override
    public OperatorMemoryAllocatorCtx getMemoryAllocatorCtx() {
        return memoryAllocatorCtx;
    }

    // -------------------------------------------------------------------------

    void spillCurrentBuffer() throws IOException {
        if (inputBuffer.isEmpty()) {
            return;
        }
        TupleSpillFile sf = new TupleSpillFile(SpillManager.INSTANCE.createSpillFile(), schema);
        sf.write(inputBuffer);
        sf.finishWrite();
        spilledCount += inputBuffer.size();
        LogUtils.debug(log, "Spilled {} aggregate input tuples to {}, totalSpilled={}",
            inputBuffer.size(), sf.getFile().getName(), spilledCount);
        spillFiles.add(sf);
        inputBuffer.clear();
    }
}

