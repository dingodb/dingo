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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.profile.Profile;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.DingoCompileContext;
import io.dingodb.exec.expr.DingoRelConfig;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.operator.spill.SpillManager;
import io.dingodb.exec.operator.spill.TupleSpillFile;
import io.dingodb.exec.tuple.TupleKey;
import io.dingodb.expr.common.type.TupleType;
import io.dingodb.expr.rel.RelOp;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Parameters for the HashJoin operator.
 *
 * <p>When {@code rightSchema} and {@code leftSchema} are set and the right-side build
 * exceeds {@code maxBuildSize} tuples, the operator switches to a <em>grace hash join</em>
 * strategy:
 * <ol>
 *   <li>The right input is hash-partitioned into {@code numPartitions} buckets.</li>
 *   <li>When the in-memory right-side count exceeds {@code maxBuildSize}, the largest
 *       partition(s) are spilled to temporary files.</li>
 *   <li>Left-side tuples whose matching right partition has been spilled are likewise
 *       written to a corresponding left spill file.</li>
 *   <li>After both sides complete, each spilled partition pair is joined in-memory
 *       (one partition at a time), bounding peak memory to one partition's worth of data.</li>
 * </ol>
 *
 * <p>If {@code rightSchema}/{@code leftSchema} are {@code null} (the default), the operator
 * behaves exactly as before – holding the entire right side in memory.
 */
@Getter
@Slf4j
@JsonTypeName("hashJoin")
@JsonPropertyOrder({"joinType", "leftMapping", "rightMapping"})
public class HashJoinParam extends AbstractParams {

    /** Default number of hash partitions used during grace hash join. */
    static final int DEFAULT_NUM_PARTITIONS = 16;

    @JsonProperty("leftMapping")
    private final TupleMapping leftMapping;
    @JsonProperty("rightMapping")
    private final TupleMapping rightMapping;
    // For OUTER join, there may be no input tuples, so the length of tuple cannot be achieved.
    @JsonProperty("leftLength")
    private final int leftLength;
    @JsonProperty("rightLength")
    private final int rightLength;
    @JsonProperty("leftRequired")
    private final boolean leftRequired;
    @JsonProperty("rightRequired")
    private final boolean rightRequired;

    /**
     * Maximum number of right-side tuples to hold in memory before grace-hash-join spill
     * activates.  A value of {@code 0} disables spill (all right tuples stay in memory).
     */
    @JsonProperty("maxBuildSize")
    @Setter
    private int maxBuildSize;

    /** Number of hash partitions for grace hash join (ignored when spill is disabled). */
    @JsonProperty("numPartitions")
    @Setter
    private int numPartitions;

    /**
     * Schema of the right-side tuples, used for spill encoding.
     * Must be set (non-null) to enable hash-join spill.
     */
    @Getter
    @Setter
    private DingoType rightSchema;

    /**
     * Schema of the left-side tuples, used for spill encoding.
     * Must be set (non-null) to enable hash-join spill.
     */
    @Getter
    @Setter
    private DingoType leftSchema;

    @Setter
    private transient boolean rightFinFlag;
    private transient ConcurrentHashMap<TupleKey, List<TupleWithJoinFlag>> hashMap;
    @Setter
    private transient CompletableFuture<Void> future;

    /** Running count of right tuples currently in {@link #hashMap}. */
    private transient int rightInMemoryCount;

    /**
     * Spilled right-side partition files: partition index → spill file.
     * Populated only when right-side spill occurs.
     */
    private transient Map<Integer, TupleSpillFile> spilledRightPartitions;

    /**
     * Spilled left-side partition files: partition index → spill file.
     * Populated only when the matching right partition has been spilled.
     */
    private transient Map<Integer, TupleSpillFile> spilledLeftPartitions;

    @Setter
    public Profile profileLeft;
    @Setter
    public Profile profileRight;

    @Setter
    public SqlExpr otherExpr;

    @Setter
    public RelOp relOp;
    public DingoRelConfig config;
    @Setter
    public DingoType schema;

    @Setter
    public String joinType;

    public boolean leftMappingEmpty;
    public boolean rightMappingEmpty;

    private volatile boolean interrupted = false;


    public HashJoinParam(
        TupleMapping leftMapping,
        TupleMapping rightMapping,
        int leftLength,
        int rightLength,
        boolean leftRequired,
        boolean rightRequired
    ) {
        this.leftMapping = leftMapping;
        this.rightMapping = rightMapping;
        this.leftLength = leftLength;
        this.rightLength = rightLength;
        this.leftRequired = leftRequired;
        this.rightRequired = rightRequired;
        this.leftMappingEmpty = this.leftMapping.size() == 0;
        this.rightMappingEmpty = this.rightMapping.size() == 0;
        this.config = new DingoRelConfig();
        this.numPartitions = DEFAULT_NUM_PARTITIONS;
    }

    public static TupleKey rtrimTupleKey(TupleKey key) {
        ArrayList<Object> arrayList = new ArrayList<>();
        Arrays.stream(key.getTuple()).forEach(
            obj -> {
                if (obj instanceof String) {
                    String str = (String) obj;
                    int blankCount = 0;
                    for ( int i = str.length() - 1; i >= 0; i-- ) {
                        if ( Character.isWhitespace(str.charAt(i)) ) {
                            blankCount++;
                        }
                    }
                    str = str.substring(0, str.length() - blankCount);
                    arrayList.add(str);
                } else {
                    arrayList.add(obj);
                }
            }
        );

        return new TupleKey(arrayList.toArray());
    }

    public static Object[] rtrimTuple(Object[] tuple) {
        ArrayList<Object> arrayList = new ArrayList<>();
        Arrays.stream(tuple).forEach(
            obj -> {
                if (obj instanceof String) {
                    String str = (String) obj;
                    int blankCount = 0;
                    for (int i = str.length() - 1; i >= 0; i--) {
                        if (Character.isWhitespace(str.charAt(i))) {
                            blankCount++;
                        } else {
                            break;
                        }
                    }
                    str = str.substring(0, str.length() - blankCount);
                    arrayList.add(str);
                } else {
                    arrayList.add(obj);
                }
            }
        );

        return arrayList.toArray();
    }

    public static boolean containsNull(TupleKey key) {
        for (Object item : key.getTuple()) {
            if (item == null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void init(Vertex vertex) {
        rightFinFlag = false;
        hashMap = new ConcurrentHashMap<>();
        future = new CompletableFuture<>();
        rightInMemoryCount = 0;
        spilledRightPartitions = new HashMap<>();
        spilledLeftPartitions = new HashMap<>();
        if (relOp != null) {
            relOp = relOp.compile(new DingoCompileContext(
                (TupleType) schema.getType(),
                (TupleType) vertex.getParasType().getType()
            ), config);
        }
    }

    public void clear() {
        rightFinFlag = false;
        hashMap.clear();
        rightInMemoryCount = 0;
        closeSpillFiles();
        future = new CompletableFuture<>();
    }

    public void interrupt() {
        this.interrupted = true;
        LogUtils.warn(log, "HashJoin operation interrupted");
        if (!future.isDone()) {
            future.completeExceptionally(new InterruptedException("HashJoin operation interrupted"));
        }
    }

    // -------------------------------------------------------------------------
    // Grace hash join helpers
    // -------------------------------------------------------------------------

    /**
     * Returns whether spill-to-disk is enabled for this hash join.
     * Spill is enabled when {@code rightSchema} and {@code leftSchema} are set and
     * {@code maxBuildSize} is positive.
     */
    public boolean isSpillEnabled() {
        return rightSchema != null && leftSchema != null && maxBuildSize > 0;
    }

    /**
     * Returns whether any right-side partitions have been spilled to disk.
     */
    public boolean hasSpilledPartitions() {
        return !spilledRightPartitions.isEmpty();
    }

    /**
     * Computes the partition index for a join key tuple.
     *
     * @param key the (trimmed) join key
     * @return a non-negative partition index in {@code [0, numPartitions)}
     */
    public int partitionOf(TupleKey key) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

    /**
     * Adds a right-side tuple to the hash map and increments the in-memory counter.
     * When the counter reaches {@code maxBuildSize}, the largest partition is spilled.
     *
     * @param key   the (trimmed) right join key
     * @param tuple the full right tuple
     * @throws IOException if spilling fails
     */
    public void addRightTuple(TupleKey key, Object[] tuple) throws IOException {
        List<TupleWithJoinFlag> list = hashMap
            .computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>()));
        list.add(new TupleWithJoinFlag(tuple));
        rightInMemoryCount++;
        if (isSpillEnabled() && rightInMemoryCount >= maxBuildSize) {
            spillLargestRightPartition();
        }
    }

    /**
     * Writes a left-side tuple to its spill file when the matching right partition is on disk.
     * Returns {@code true} if the left tuple was spilled (and should not be probed in-memory).
     *
     * @param key        the (trimmed) left join key
     * @param leftTuple  the full left tuple
     * @return {@code true} if the tuple was spilled, {@code false} if it should be probed normally
     * @throws IOException if writing to the spill file fails
     */
    public boolean spillLeftTupleIfNeeded(TupleKey key, Object[] leftTuple) throws IOException {
        if (spilledRightPartitions.isEmpty()) {
            return false;
        }
        int partition = partitionOf(key);
        if (!spilledRightPartitions.containsKey(partition)) {
            return false;
        }
        TupleSpillFile leftFile = spilledLeftPartitions.computeIfAbsent(partition, p -> {
            try {
                return new TupleSpillFile(SpillManager.INSTANCE.createSpillFile(), leftSchema);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create left spill file for partition " + p, e);
            }
        });
        leftFile.write(Collections.singletonList(leftTuple));
        return true;
    }

    /**
     * Returns the set of partition indices that have been spilled to disk.
     */
    public java.util.Set<Integer> getSpilledPartitionIds() {
        return spilledRightPartitions.keySet();
    }

    /**
     * Returns the right spill file for the given partition, or {@code null} if not spilled.
     */
    public TupleSpillFile getSpilledRightFile(int partition) {
        return spilledRightPartitions.get(partition);
    }

    /**
     * Returns the left spill file for the given partition, or {@code null} if no left tuples
     * were spilled for it.
     */
    public TupleSpillFile getSpilledLeftFile(int partition) {
        return spilledLeftPartitions.get(partition);
    }

    // -------------------------------------------------------------------------

    /**
     * Identifies the partition with the highest in-memory tuple count and spills it to disk.
     * All TupleKeys belonging to that partition are removed from {@link #hashMap}.
     */
    private void spillLargestRightPartition() throws IOException {
        // Count tuples per partition
        int[] partitionCounts = new int[numPartitions];
        for (Map.Entry<TupleKey, List<TupleWithJoinFlag>> entry : hashMap.entrySet()) {
            int p = partitionOf(entry.getKey());
            partitionCounts[p] += entry.getValue().size();
        }
        // Find the largest non-yet-spilled partition
        int maxPartition = -1;
        int maxCount = 0;
        for (int p = 0; p < numPartitions; p++) {
            if (!spilledRightPartitions.containsKey(p) && partitionCounts[p] > maxCount) {
                maxCount = partitionCounts[p];
                maxPartition = p;
            }
        }
        if (maxPartition < 0 || maxCount == 0) {
            return; // Nothing to spill
        }
        spillRightPartition(maxPartition);
    }

    /**
     * Spills all right-side tuples belonging to the given partition to a new spill file,
     * then removes those entries from the in-memory hash map.
     */
    private void spillRightPartition(int partition) throws IOException {
        TupleSpillFile spillFile = new TupleSpillFile(
            SpillManager.INSTANCE.createSpillFile(), rightSchema
        );
        int count = 0;
        Iterator<Map.Entry<TupleKey, List<TupleWithJoinFlag>>> iter = hashMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<TupleKey, List<TupleWithJoinFlag>> entry = iter.next();
            if (partitionOf(entry.getKey()) == partition) {
                for (TupleWithJoinFlag t : entry.getValue()) {
                    spillFile.write(Collections.singletonList(t.getTuple()));
                    count++;
                }
                iter.remove();
                rightInMemoryCount -= entry.getValue().size();
            }
        }
        spillFile.finishWrite();
        spilledRightPartitions.put(partition, spillFile);
        LogUtils.debug(log, "Spilled right partition {} to disk: {} tuples", partition, count);
    }

    private void closeSpillFiles() {
        if (spilledRightPartitions != null) {
            spilledRightPartitions.values().forEach(TupleSpillFile::close);
            spilledRightPartitions.clear();
        }
        if (spilledLeftPartitions != null) {
            spilledLeftPartitions.values().forEach(TupleSpillFile::close);
            spilledLeftPartitions.clear();
        }
    }
}
