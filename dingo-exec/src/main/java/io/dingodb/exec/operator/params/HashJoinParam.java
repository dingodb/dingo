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
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.tuple.TupleKey;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Getter
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
    }

    @Override
    public void init(Vertex vertex) {
        rightFinFlag = false;
        hashMap = new ConcurrentHashMap<>();
        future = new CompletableFuture<>();
    }

    public void clear() {
        hashMap.clear();
    }
}
