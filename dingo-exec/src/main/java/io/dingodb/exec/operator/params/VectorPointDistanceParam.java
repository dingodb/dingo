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
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.spill.SpillFile;
import io.dingodb.exec.spill.SpillFileManager;
import io.dingodb.exec.spill.TupleSerializer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Getter
@Slf4j
@JsonTypeName("vectorPoint")
@JsonPropertyOrder({"dimension"})
public class VectorPointDistanceParam extends AbstractParams {

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

    // Spill support
    /** Number of in-memory tuples that triggers a spill. Disabled (MAX_VALUE) by default. */
    private transient long spillThreshold = Long.MAX_VALUE;
    private transient List<SpillFile> spillFiles = new ArrayList<>();
    private transient int spillBucketCounter = 0;

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
        this.rangeDistribution = rangeDistribution;
        this.vectorIndex = vectorIndex;
        this.isBinaryVector = isBinaryVector;
        this.targetVector = targetVector;
        this.binaryVector = binaryVector;
        this.dimension = dimension;
        this.algType = algType;
        this.metricType = metricType;
        this.indexTableId = indexTableId;
        cache = new LinkedList<>();
        this.selection = selection;
        this.topk = topk;
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
     * @param operatorId logical identifier for this operator instance
     * @throws IOException on spill I/O failure
     */
    public void spillCache(String operatorId) throws IOException {
        if (cache.isEmpty()) {
            return;
        }
        SpillFileManager mgr = SpillFileManager.getInstance();
        SpillFile sf = mgr.createSpill(operatorId, spillBucketCounter++);
        mgr.write(sf, TupleSerializer.serialize(cache));
        mgr.closeAndFlush(sf);
        spillFiles.add(sf);
        log.debug("VectorPointDistanceParam: spilled {} tuples to {}", cache.size(), sf.getFile());
        cache.clear();
    }

    /**
     * Reads all previously spilled tuples into the in-memory cache and releases the spill files.
     *
     * @throws IOException on read failure
     */
    public void restoreSpilledTuples() throws IOException {
        SpillFileManager mgr = SpillFileManager.getInstance();
        for (SpillFile sf : spillFiles) {
            byte[] data = mgr.readAllBytes(sf);
            if (data.length > 0) {
                cache.addAll(TupleSerializer.deserialize(data));
            }
            mgr.release(sf);
        }
        spillFiles.clear();
    }

    /** Returns {@code true} if there are any tuples spilled to disk. */
    public boolean hasSpilledData() {
        return !spillFiles.isEmpty();
    }

    public void clear() {
        cache.clear();
        if (!spillFiles.isEmpty()) {
            SpillFileManager mgr = SpillFileManager.getInstance();
            for (SpillFile sf : spillFiles) {
                mgr.release(sf);
            }
            spillFiles.clear();
        }
        spillBucketCounter = 0;
    }

}
