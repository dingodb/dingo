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
import io.dingodb.exec.spill.SpillConfig;
import io.dingodb.exec.spill.SpillFile;
import io.dingodb.exec.spill.SpillFileManager;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

@Getter
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

    /** Spill support – created lazily when the first spill occurs. */
    private transient SpillFileManager spillFileManager;
    /** Ordered list of spill files written so far. */
    private transient List<SpillFile> spillFiles;
    /** How many in-memory rows to accumulate before spilling. */
    private transient int spillThreshold;

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
        this.spillFiles = new ArrayList<>();
        this.spillThreshold = SpillConfig.getSortSpillThreshold();
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
        // Re-initialise transient spill state for each execution (in case the param
        // is reused across multiple task runs after serialisation/deserialisation).
        spillFiles = new ArrayList<>();
        spillThreshold = SpillConfig.getSortSpillThreshold();
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
     * Returns {@code true} when the in-memory cache should be spilled to disk.
     */
    public boolean shouldSpill() {
        return cache.size() >= spillThreshold;
    }

    /**
     * Returns (or lazily creates) the {@link SpillFileManager} for this operator instance.
     */
    public SpillFileManager getOrCreateSpillFileManager() {
        if (spillFileManager == null) {
            spillFileManager = new SpillFileManager();
        }
        return spillFileManager;
    }

    public void addSpillFile(SpillFile sf) {
        spillFiles.add(sf);
    }

    public boolean hasSpillFiles() {
        return !spillFiles.isEmpty();
    }

    public void clear() {
        cache.clear();
        // Delete all spill files to free disk space.
        if (spillFileManager != null && spillFiles != null) {
            for (SpillFile sf : spillFiles) {
                spillFileManager.delete(sf);
            }
        }
        spillFiles = new ArrayList<>();
    }

    public OperatorProfile getProfile() {
        return new OperatorProfile("sort");
    }
}
