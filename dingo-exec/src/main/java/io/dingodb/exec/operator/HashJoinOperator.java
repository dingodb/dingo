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
import io.dingodb.exec.tuple.TupleKey;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@JsonTypeName("hashJoin")
@JsonPropertyOrder({"leftMapping", "rightMapping"})
public class HashJoinOperator extends SoleOutOperator {
    @JsonProperty("leftMapping")
    private final TupleMapping leftMapping;
    @JsonProperty("rightMapping")
    private final TupleMapping rightMapping;
    boolean rightFinFlag;
    private ConcurrentHashMap<TupleKey, List<Object[]>> hashMap;

    @JsonCreator
    public HashJoinOperator(
        @JsonProperty("leftMapping") TupleMapping leftMapping,
        @JsonProperty("rightMapping") TupleMapping rightMapping
    ) {
        this.leftMapping = leftMapping;
        this.rightMapping = rightMapping;
        rightFinFlag = false;
    }

    @Override
    public void init() {
        super.init();
        hashMap = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized boolean push(int pin, Object[] tuple) {
        if (pin == 0) { // left
            if (!rightFinFlag) {
                while (true) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        continue;
                    }
                    break;
                }
            }
            TupleKey leftKey = new TupleKey(leftMapping.revMap(tuple));
            List<Object[]> rightList = hashMap.get(leftKey);
            if (rightList != null) {
                for (Object[] t : rightList) {
                    Object[] newTuple = Arrays.copyOf(tuple, tuple.length + t.length);
                    System.arraycopy(t, 0, newTuple, tuple.length, t.length);
                    if (!output.push(newTuple)) {
                        return false;
                    }
                }
            }
        } else if (pin == 1) { //right
            TupleKey rightKey = new TupleKey(rightMapping.revMap(tuple));
            List<Object[]> list = hashMap.computeIfAbsent(rightKey, k -> new LinkedList<>());
            list.add(tuple);
        }
        return true;
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        if (pin == 0) { // left
            output.fin(fin);
        } else if (pin == 1) { //right
            rightFinFlag = true;
            notify();
        }
    }
}
