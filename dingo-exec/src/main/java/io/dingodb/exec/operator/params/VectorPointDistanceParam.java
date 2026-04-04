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
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryManager;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolUtils;
import io.dingodb.common.memory.QueryMemoryPool;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.memory.OperatorMemoryAllocatorCtx;
import io.dingodb.exec.operator.spill.SpillManager;
import io.dingodb.exec.operator.spill.TupleSpillFile;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Getter
@JsonTypeName("vectorPoint")
@JsonPropertyOrder({"dimension"})
public class VectorPointDistanceParam extends AbstractParams implements RevokerParams {

    private final RangeDistribution rangeDistribution;

    private final Integer vectorIndex;

    private final boolean isBinaryVector;

    private final List<Float> targetVector;

    private final byte[] binaryVector;
    @JsonProperty("dimension")
    private final Integer dimension;

    private final String algType;

    private final String metricType;

    private final CommonId indexTableId;

    private final List<Object[]> cache;

    private final TupleMapping selection;

    private final Integer topk;

    /**
     * Optional input row schema used to serialize spill data.
     * When {@code null}, spill-to-disk is disabled.
     */
    private final DingoType schema;

    /**
     * Maximum number of tuples held in memory before spilling to disk.
     * A value of {@code 0} means "use the default from {@link SpillManager}".
     */
    private final int spillThreshold;

    /** Spill files for input runs written to disk; {@code null} when spill is disabled. */
    private transient List<TupleSpillFile> spillFiles;
    /** Total number of tuples already written to spill files. */
    private transient long spilledCount;
    /** Per-operator query-level memory pool (for scheduler integration). */
    private transient QueryMemoryPool queryMemoryPool;
    /** Memory allocator context used by the memory-revoking scheduler. */
    private transient OperatorMemoryAllocatorCtx memoryAllocatorCtx;

    public VectorPointDistanceParam(
        RangeDistribution rangeDistribution,
        Integer vectorIndex,
        CommonId indexTableId,
        boolean isBinaryVector,
        List<Float> targetVector,
        byte[] binaryVector,
        Integer dimension,
        String algType,
        String metricType,
        Integer topk,
        TupleMapping selection,
        DingoType schema,
        int spillThreshold
    ) {
        this.rangeDistribution = rangeDistribution;
        this.vectorIndex = vectorIndex;
        this.isBinaryVector = isBinaryVector;
        this.targetVector = targetVector;
        this.binaryVector = binaryVector;
        this.dimension = dimension;
        this.algType = algType;
        this.metricType = metricType;
        this.indexTableId = indexTableId;
        // ArrayList is used for O(1) indexed access needed in the distance calculation loop
        this.cache = new ArrayList<>();
        this.selection = selection;
        this.topk = topk;
        this.schema = schema;
        this.spillThreshold = spillThreshold;
        if (schema != null) {
            this.spillFiles = new ArrayList<>();
            if (ScopeVariables.enableSpill()) {
                String poolName = "vectorPoint-" + UUID.randomUUID();
                queryMemoryPool = (QueryMemoryPool) MemoryManager.getInstance()
                    .createQueryMemoryPool(false, poolName);
                MemoryPool opPool = MemoryPoolUtils.createOperatorTmpTablePool(
                    poolName + "-op", queryMemoryPool);
                memoryAllocatorCtx = new OperatorMemoryAllocatorCtx(opPool, true);
            }
        }
    }

    /** Convenience constructor for callers that do not need spill support (backward compat). */
    public VectorPointDistanceParam(
        RangeDistribution rangeDistribution,
        Integer vectorIndex,
        CommonId indexTableId,
        boolean isBinaryVector,
        List<Float> targetVector,
        byte[] binaryVector,
        Integer dimension,
        String algType,
        String metricType,
        Integer topk,
        TupleMapping selection
    ) {
        this(rangeDistribution, vectorIndex, indexTableId, isBinaryVector, targetVector, binaryVector,
            dimension, algType, metricType, topk, selection, null, 0);
    }

    /** Returns the effective spill threshold (always positive). */
    public int getEffectiveSpillThreshold() {
        return spillThreshold > 0 ? spillThreshold : SpillManager.DEFAULT_SPILL_THRESHOLD;
    }

    /** Returns whether spill-to-disk is enabled for this parameter set. */
    public boolean isSpillEnabled() {
        return schema != null;
    }

    /** Returns whether any tuples have already been spilled to disk. */
    public boolean hasSpillFiles() {
        return spillFiles != null && !spillFiles.isEmpty();
    }

    /**
     * Writes the current in-memory cache as a spill file and clears the cache.
     *
     * @throws IOException if the spill file cannot be created or written
     */
    public void spillCurrentBatch() throws IOException {
        if (cache.isEmpty()) {
            return;
        }
        TupleSpillFile sf = new TupleSpillFile(SpillManager.INSTANCE.createSpillFile(), schema);
        sf.write(cache);
        sf.finishWrite();
        spilledCount += cache.size();
        LogUtils.debug(log, "Spilled {} VectorPointDistance tuples to {}, totalSpilled={}",
            cache.size(), sf.getFile().getName(), spilledCount);
        spillFiles.add(sf);
        cache.clear();
    }

    /**
     * Returns all tuples: those in the in-memory cache plus any spilled to disk.
     * Spill files are closed after reading. The returned list includes all tuples
     * in insertion order (spilled files first, then in-memory cache).
     *
     * @throws IOException if a spill file cannot be read
     */
    public List<Object[]> getAllTuples() throws IOException {
        if (!hasSpillFiles()) {
            return cache;
        }
        List<Object[]> all = new ArrayList<>();
        for (TupleSpillFile sf : spillFiles) {
            sf.iterator().forEachRemaining(all::add);
            sf.close();
        }
        spillFiles.clear();
        all.addAll(cache);
        return all;
    }

    public void clear() {
        cache.clear();
        spilledCount = 0;
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

}

