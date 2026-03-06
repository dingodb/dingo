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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryManager;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolUtils;
import io.dingodb.common.memory.QueryMemoryPool;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.memory.OperatorMemoryAllocatorCtx;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.operator.spill.SpillManager;
import io.dingodb.exec.operator.spill.TupleSpillFile;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/**
 * Parameters for the Sort operator.
 *
 * <p>When {@code schema} is provided and the in-memory {@code cache} reaches
 * {@code spillThreshold} tuples, the sorted batch is written to a temporary spill file and
 * the cache is cleared.  On finalization ({@code fin()}), all sorted runs – both in-memory
 * and on-disk – are merged using a K-way merge sort before being emitted downstream.
 *
 * <p>If {@code schema} is {@code null} (or the threshold is never reached), the operator
 * behaves exactly as before, keeping everything in memory.
 */
@Getter
@Slf4j
@JsonTypeName("sort")
@JsonPropertyOrder({"collations", "limit", "offset", "vectorHybrid", "schema", "spillThreshold"})
public class SortParam extends AbstractParams implements RevokerParams {

    @JsonProperty("collations")
    private final List<SortCollation> collations;
    @JsonProperty("limit")
    private final int limit;
    @JsonProperty("offset")
    private final int offset;
    @JsonProperty("vectorHybrid")
    private final boolean vectorHybrid;
    /**
     * Optional tuple schema used to serialize spill data.
     * When {@code null}, spill-to-disk is disabled.
     */
    @JsonProperty("schema")
    private final DingoType schema;
    /**
     * Maximum number of tuples held in memory before a sorted run is spilled to disk.
     * A value of {@code 0} means "use the default from {@link SpillManager}".
     */
    @JsonProperty("spillThreshold")
    private final int spillThreshold;

    /** In-memory tuple buffer. */
    private final List<Object[]> cache;
    /** Spill files for sorted runs written to disk; {@code null} when spill is disabled. */
    private transient List<TupleSpillFile> spillFiles;
    /** Total number of tuples already written to spill files. */
    private transient long spilledCount;
    /** Per-operator query-level memory pool (for scheduler integration). */
    private transient QueryMemoryPool queryMemoryPool;
    /** Memory allocator context used by the memory-revoking scheduler. */
    private transient OperatorMemoryAllocatorCtx memoryAllocatorCtx;

    private transient Comparator<Object[]> comparator;

    @JsonCreator
    public SortParam(
        @JsonProperty("collations") @NonNull List<SortCollation> collations,
        @JsonProperty("limit") int limit,
        @JsonProperty("offset") int offset,
        @JsonProperty("vectorHybrid") boolean vectorHybrid,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("spillThreshold") int spillThreshold
    ) {
        this.collations = collations;
        this.limit = limit;
        this.offset = offset;
        this.vectorHybrid = vectorHybrid;
        this.schema = schema;
        this.spillThreshold = spillThreshold;
        this.cache = new ArrayList<>();
        this.spilledCount = 0;
        if (schema != null) {
            this.spillFiles = new ArrayList<>();
        }
        comparator = buildComparator(collations);
    }

    /** Convenience constructor for callers that do not need spill support (backward compat). */
    public SortParam(
        @NonNull List<SortCollation> collations,
        int limit,
        int offset,
        boolean vectorHybrid
    ) {
        this(collations, limit, offset, vectorHybrid, null, 0);
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        comparator = buildComparator(collations);
        if (schema != null) {
            spillFiles = new ArrayList<>();
            spilledCount = 0;
            // Create a per-operator memory pool for the revocation scheduler
            if (ScopeVariables.enableSpill()) {
                String poolName = "sort-" + UUID.randomUUID();
                queryMemoryPool = (QueryMemoryPool) MemoryManager.getInstance()
                    .createQueryMemoryPool(false, poolName);
                MemoryPool opPool = MemoryPoolUtils.createOperatorTmpTablePool(
                    poolName + "-op", queryMemoryPool);
                memoryAllocatorCtx = new OperatorMemoryAllocatorCtx(opPool, true);
            }
        }
    }

    /**
     * Returns the effective spill threshold (always positive).
     */
    public int getEffectiveSpillThreshold() {
        return spillThreshold > 0 ? spillThreshold : SpillManager.DEFAULT_SPILL_THRESHOLD;
    }

    /**
     * Returns whether spill-to-disk is enabled for this parameter set.
     */
    public boolean isSpillEnabled() {
        return schema != null;
    }

    /**
     * Returns whether any tuples have already been spilled to disk.
     */
    public boolean hasSpillFiles() {
        return spillFiles != null && !spillFiles.isEmpty();
    }

    /**
     * Sorts the current in-memory cache and writes it as a sorted run to a new spill file.
     * Clears the in-memory cache afterwards.
     *
     * @throws IOException if the spill file cannot be created or written
     */
    public void spillCurrentBatch() throws IOException {
        if (cache.isEmpty()) {
            return;
        }
        if (comparator != null) {
            cache.sort(comparator);
        }
        TupleSpillFile spillFile = new TupleSpillFile(SpillManager.INSTANCE.createSpillFile(), schema);
        spillFile.write(cache);
        spillFile.finishWrite();
        spilledCount += cache.size();
        LogUtils.debug(log, "Spilled {} tuples to {}, totalSpilled={}", cache.size(),
            spillFile.getFile().getName(), spilledCount);
        spillFiles.add(spillFile);
        cache.clear();
    }

    /**
     * Returns the total number of tuples that have been spilled to disk (not counting the
     * current in-memory batch).
     */
    public long getSpilledCount() {
        return spilledCount;
    }

    /**
     * Clears the in-memory cache and closes / deletes all spill files.
     */
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

    public OperatorProfile getProfile() {
        return new OperatorProfile("sort");
    }

    // -------------------------------------------------------------------------

    private static Comparator<Object[]> buildComparator(List<SortCollation> collations) {
        if (collations.isEmpty()) {
            return null;
        }
        Comparator<Object[]> c = collations.get(0).makeComparator();
        for (int i = 1; i < collations.size(); ++i) {
            c = c.thenComparing(collations.get(i).makeComparator());
        }
        return c;
    }
}
