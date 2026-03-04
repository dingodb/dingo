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
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.spill.SpillFile;
import io.dingodb.exec.spill.SpillFileManager;
import io.dingodb.exec.spill.TupleSerializer;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

@Getter
@Slf4j
@JsonTypeName("sort")
@JsonPropertyOrder({"collations", "limit", "offset", "vectorHybrid"})
public class SortParam extends AbstractParams {

    @JsonProperty("collations")
    private final List<SortCollation> collations;
    @JsonProperty("limit")
    private final int limit;
    @JsonProperty("offset")
    private final int offset;
    @JsonProperty("vectorHybrid")
    private final boolean vectorHybrid;
    private final List<Object[]> cache;
    private transient Comparator<Object[]> comparator;

    // Spill support
    /** Number of in-memory tuples that triggers a spill to disk. Disabled (MAX_VALUE) by default. */
    private transient long spillThreshold = Long.MAX_VALUE;
    private transient List<SpillFile> spillFiles;

    @JsonCreator
    public SortParam(
        @JsonProperty("collations") @NonNull List<SortCollation> collations,
        @JsonProperty("limit") int limit,
        @JsonProperty("offset") int offset,
        @JsonProperty("vectorHybrid") boolean vectorHybrid
    ) {
        this.collations = collations;
        this.limit = limit;
        this.offset = offset;
        this.vectorHybrid = vectorHybrid;
        this.cache = new LinkedList<>();
        if (!collations.isEmpty()) {
            Comparator<Object[]> c = collations.get(0).makeComparator();
            for (int i = 1; i < collations.size(); ++i) {
                c = c.thenComparing(collations.get(i).makeComparator());
            }
            comparator = c;
        } else {
            comparator = null;
        }
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        spillFiles = new ArrayList<>();
        if (!collations.isEmpty()) {
            Comparator<Object[]> c = collations.get(0).makeComparator();
            for (int i = 1; i < collations.size(); ++i) {
                c = c.thenComparing(collations.get(i).makeComparator());
            }
            comparator = c;
        } else {
            comparator = null;
        }
    }

    /**
     * Sets the in-memory tuple threshold above which the cache will be spilled to disk.
     * A value of {@link Long#MAX_VALUE} (the default) disables spilling.
     *
     * @param threshold maximum number of in-memory tuples before spill
     */
    public void setSpillThreshold(long threshold) {
        this.spillThreshold = threshold;
    }

    /**
     * Spills the current in-memory cache to a new spill file and clears the cache.
     * Does nothing if the cache is empty.
     *
     * @param mgr        the {@link SpillFileManager} to use
     * @param operatorId logical identifier for this operator instance
     * @param bucketId   partition/bucket index
     * @throws IOException on spill I/O failure
     */
    public void spillCache(SpillFileManager mgr, String operatorId, int bucketId) throws IOException {
        if (cache.isEmpty()) {
            return;
        }
        SpillFile sf = mgr.createSpill(operatorId, bucketId);
        mgr.write(sf, TupleSerializer.serialize(cache));
        mgr.closeAndFlush(sf);
        spillFiles.add(sf);
        log.debug("SortParam: spilled {} tuples to {}", cache.size(), sf.getFile());
        cache.clear();
    }

    /**
     * Reads all previously spilled tuples back into a list and releases the spill files.
     *
     * @param mgr the {@link SpillFileManager} to use
     * @return all tuples recovered from spill files
     * @throws IOException on read failure
     */
    public List<Object[]> readSpilledTuples(SpillFileManager mgr) throws IOException {
        List<Object[]> result = new ArrayList<>();
        for (SpillFile sf : spillFiles) {
            byte[] data = mgr.readAllBytes(sf);
            if (data.length > 0) {
                result.addAll(TupleSerializer.deserialize(data));
            }
            mgr.release(sf);
        }
        spillFiles.clear();
        return result;
    }

    /** Returns {@code true} if there are any tuples spilled to disk. */
    public boolean hasSpilledData() {
        return spillFiles != null && !spillFiles.isEmpty();
    }

    public void clear() {
        cache.clear();
        if (spillFiles != null && !spillFiles.isEmpty()) {
            SpillFileManager mgr = SpillFileManager.getInstance();
            for (SpillFile sf : spillFiles) {
                mgr.release(sf);
            }
            spillFiles.clear();
        }
    }

    public OperatorProfile getProfile() {
        return new OperatorProfile("sort");
    }
}
