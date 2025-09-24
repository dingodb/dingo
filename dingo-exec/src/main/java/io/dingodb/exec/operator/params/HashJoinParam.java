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
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.tuple.TupleKey;
import io.dingodb.expr.rel.RelOp;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    }

    public void clear() {
        rightFinFlag = false;
        hashMap.clear();
        future = new CompletableFuture<>();
    }

    public void interrupt() {
        this.interrupted = true;
        LogUtils.warn(log, "HashJoin operation interrupted");
        if (!future.isDone()) {
            future.completeExceptionally(new InterruptedException("HashJoin operation interrupted"));
        }
    }
}
