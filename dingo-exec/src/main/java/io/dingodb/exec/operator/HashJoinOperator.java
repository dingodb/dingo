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

package io.dingodb.exec.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.tuple.TupleKey;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@JsonTypeName("hashJoin")
@JsonPropertyOrder({"joinType", "leftMapping", "rightMapping"})
public class HashJoinOperator extends SoleOutOperator {
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

    boolean rightFinFlag;
    private ConcurrentHashMap<TupleKey, List<TupleWithJoinFlag>> hashMap;

    @JsonCreator
    public HashJoinOperator(
        @JsonProperty("leftMapping") TupleMapping leftMapping,
        @JsonProperty("rightMapping") TupleMapping rightMapping,
        @JsonProperty("leftLength") int leftLength,
        @JsonProperty("rightLength") int rightLength,
        @JsonProperty("leftRequired") boolean leftRequired,
        @JsonProperty("rightRequired") boolean rightRequired
    ) {
        this.leftMapping = leftMapping;
        this.rightMapping = rightMapping;
        this.leftLength = leftLength;
        this.rightLength = rightLength;
        this.leftRequired = leftRequired;
        this.rightRequired = rightRequired;
        rightFinFlag = false;
    }

    @Override
    public void init() {
        super.init();
        hashMap = new ConcurrentHashMap<>();
    }

    private void waitRightFinFlag() {
        while (!rightFinFlag) {
            try {
                wait();
            } catch (InterruptedException ignored) {
            }
        }
    }

    @Override
    public synchronized boolean push(int pin, Object[] tuple) {
        if (pin == 0) { // left
            waitRightFinFlag();
            TupleKey leftKey = new TupleKey(leftMapping.revMap(tuple));
            List<TupleWithJoinFlag> rightList = hashMap.get(leftKey);
            if (rightList != null) {
                for (TupleWithJoinFlag t : rightList) {
                    Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                    System.arraycopy(t.getTuple(), 0, newTuple, leftLength, rightLength);
                    t.setJoined(true);
                    if (!output.push(newTuple)) {
                        return false;
                    }
                }
            } else if (leftRequired) {
                Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                return output.push(newTuple);
            }
        } else if (pin == 1) { //right
            TupleKey rightKey = new TupleKey(rightMapping.revMap(tuple));
            List<TupleWithJoinFlag> list = hashMap.computeIfAbsent(rightKey, k -> new LinkedList<>());
            list.add(new TupleWithJoinFlag(tuple));
        }
        return true;
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        if (fin instanceof FinWithException) {
            output.fin(fin);
            return;
        }

        if (pin == 0) { // left
            if (rightRequired) {
                // should wait in case of no data push to left.
                waitRightFinFlag();
                outer:
                for (List<TupleWithJoinFlag> tList : hashMap.values()) {
                    for (TupleWithJoinFlag t : tList) {
                        if (!t.isJoined()) {
                            Object[] newTuple = new Object[leftLength + rightLength];
                            Arrays.fill(newTuple, 0, leftLength, null);
                            System.arraycopy(t.getTuple(), 0, newTuple, leftLength, rightLength);
                            if (!output.push(newTuple)) {
                                break outer;
                            }
                        }
                    }
                }
            }
            output.fin(fin);
        } else if (pin == 1) { //right
            rightFinFlag = true;
            notify();
        }
    }
}
