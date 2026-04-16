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
import com.google.common.util.concurrent.SettableFuture;
import io.dingodb.common.ExecutionContext;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.MemoryPool;
import io.dingodb.common.memory.MemoryPoolUtils;
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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Getter
@JsonTypeName("sort")
@JsonPropertyOrder({"collations", "limit", "offset", "vectorHybrid"})
public class SortParam extends AbstractParams implements RevokerParams {

    @JsonProperty("collations")
    private final List<SortCollation> collations;
    @JsonProperty("limit")
    private final int limit;
    @JsonProperty("offset")
    private final int offset;
    @JsonProperty("vectorHybrid")
    private final boolean vectorHybrid;
    @JsonProperty("schema")
    private final DingoType schema;

    private final List<Object[]> cache;
    private transient Comparator<Object[]> comparator;

    private ExecutionContext executionContext;
    private OperatorMemoryAllocatorCtx memoryAllocatorCtx;
    AtomicLong size;
    private transient String jobId;
    private transient String operatorId;
    protected long spillCnt = 0;

    @Setter
    @Getter
    private volatile boolean spilling;
    @Setter
    @Getter
    SettableFuture spillFuture;

    private transient List<TupleSpillFile> spillFiles;


    @JsonCreator
    public SortParam(
        @JsonProperty("collations") @NonNull List<SortCollation> collations,
        @JsonProperty("limit") int limit,
        @JsonProperty("offset") int offset,
        @JsonProperty("vectorHybrid") boolean vectorHybrid,
        @JsonProperty("executionContext") ExecutionContext executionContext,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("spillThreshold") int spillThreshold
    ) {
        this.collations = collations;
        this.limit = limit;
        this.offset = offset;
        this.vectorHybrid = vectorHybrid;
        this.cache = new LinkedList<>();
        this.comparator = buildComparator(collations);
        this.schema = schema;
        this.size = new AtomicLong(0);
        this.executionContext = executionContext;
        if (schema != null) {
            this.spillFiles = new ArrayList<>();
        }
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        this.jobId = vertex.getTask().getJobId().toString();
        this.operatorId = vertex.getOp().toString();
        this.comparator = buildComparator(collations);
        if (schema != null) {
            spillFiles = new ArrayList<>();
            spillCnt = 0;
        }
        if (!executionContext.isInnerSql()) {
            String name = "sort" + UUID.randomUUID();
            MemoryPool memoryPool =
                MemoryPoolUtils.createOperatorTmpTablePool(name, executionContext.getMemoryPool());
            this.memoryAllocatorCtx = new OperatorMemoryAllocatorCtx(memoryPool, ScopeVariables.enableSpill());
        }
    }

    public void clear() {
        cache.clear();
        if (this.memoryAllocatorCtx != null) {
            this.memoryAllocatorCtx.close();
        }
        spillCnt = 0;
        if (spillFiles != null) {
            for (TupleSpillFile sf : spillFiles) {
                sf.close();
            }
            spillFiles.clear();
        }
        if (spillFuture != null) {
            spillFuture.cancel(true);
            spillFuture = null;
        }
    }

    public OperatorProfile getProfile() {
        return new OperatorProfile("sort");
    }

    @Override
    public MemoryPool getQueryMemoryPool() {
        if (this.memoryAllocatorCtx != null && this.getExecutionContext() != null) {
            return this.getExecutionContext().getMemoryPool();
        }
        return null;
    }

    @Override
    public void addSpillCnt(int spillCnt) {
        this.spillCnt += spillCnt;
    }

    @Override
    public long getCacheSize() {
        return cache.size();
    }

    public boolean hasSpillFiles() {
        return spillFiles != null && !spillFiles.isEmpty();
    }

    public synchronized void spillCurrentBatch() throws IOException {
        if (cache.isEmpty()) {
            return;
        }
        if (comparator != null) {
            cache.sort(comparator);
        }
        TupleSpillFile spillFile = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile(jobId, operatorId), schema);
        spillFile.write(cache);
        spillFile.finishWrite();
        spillCnt += cache.size();
        LogUtils.debug(log, "Spilled {} tuples to {}, totalSpilled={}", cache.size(),
            spillFile.getFile().getName(), spillCnt);
        spillFiles.add(spillFile);
        cache.clear();
    }

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
