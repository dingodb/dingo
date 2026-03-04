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
import io.dingodb.exec.spill.SpillConfig;
import io.dingodb.exec.spill.SpillException;
import io.dingodb.exec.spill.SpillFile;
import io.dingodb.exec.spill.SpillFileManager;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Slf4j
@JsonTypeName("hashJoin")
@JsonPropertyOrder({"joinType", "leftMapping", "rightMapping"})
public class HashJoinParam extends AbstractParams {

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

    @Setter
    private transient boolean rightFinFlag;
    private transient ConcurrentHashMap<TupleKey, List<TupleWithJoinFlag>> hashMap;
    @Setter
    private transient CompletableFuture<Void> future;

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

    /** Spill support for the build (right) side. */
    private transient SpillFileManager spillFileManager;
    private transient List<SpillFile> buildSpillFiles;
    private transient int buildRowCount;


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
        this.buildSpillFiles = new ArrayList<>();
        this.buildRowCount = 0;
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
        buildSpillFiles = new ArrayList<>();
        buildRowCount = 0;
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
        future = new CompletableFuture<>();
        if (spillFileManager != null) {
            for (SpillFile sf : buildSpillFiles) {
                spillFileManager.delete(sf);
            }
        }
        buildSpillFiles = new ArrayList<>();
        buildRowCount = 0;
    }

    // -------------------------------------------------------------------------
    // Spill helpers for the build (right) side
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} when the build-side hash map should be spilled to disk.
     */
    public boolean shouldSpillBuild() {
        return buildRowCount >= SpillConfig.getJoinSpillThreshold();
    }

    /**
     * Spills the current build-side hash map to a spill file, then clears it.
     */
    public void spillBuildSide() {
        List<Object[]> rows = new ArrayList<>();
        for (Map.Entry<TupleKey, List<TupleWithJoinFlag>> e : hashMap.entrySet()) {
            for (TupleWithJoinFlag t : e.getValue()) {
                rows.add(t.getTuple());
            }
        }
        if (rows.isEmpty()) {
            return;
        }
        if (spillFileManager == null) {
            spillFileManager = new SpillFileManager();
        }
        SpillFile sf = spillFileManager.createSpillFile("hashJoin-build");
        try {
            spillFileManager.write(sf, rows);
        } catch (IOException ex) {
            throw new SpillException("Failed to spill hash-join build side", ex);
        }
        buildSpillFiles.add(sf);
        LogUtils.debug(log, "Spilled {} build-side rows to {}", rows.size(), sf.getPath());
        hashMap.clear();
        buildRowCount = 0;
    }

    /**
     * Restores all spilled build-side rows back into the hash map so that the probe
     * (left) side can look them up.  Called once the right (build) side has finished.
     */
    public void restoreSpilledBuildSide() {
        if (buildSpillFiles.isEmpty()) {
            return;
        }
        for (SpillFile sf : buildSpillFiles) {
            try (SpillFileManager.SpillIterator it = spillFileManager.readIterator(sf)) {
                while (it.hasNext()) {
                    Object[] tuple = it.next();
                    TupleKey key = rtrimTupleKey(new TupleKey(rightMapping.revMap(tuple)));
                    List<TupleWithJoinFlag> list = hashMap
                        .computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>()));
                    list.add(new TupleWithJoinFlag(tuple));
                }
            } catch (IOException e) {
                throw new SpillException("Failed to restore spilled build side from " + sf.getPath(), e);
            }
            spillFileManager.delete(sf);
        }
        buildSpillFiles.clear();
    }

    public void incrementBuildRowCount() {
        buildRowCount++;
    }

    public void interrupt() {
        this.interrupted = true;
        LogUtils.warn(log, "HashJoin operation interrupted");
        if (!future.isDone()) {
            future.completeExceptionally(new InterruptedException("HashJoin operation interrupted"));
        }
    }
}
