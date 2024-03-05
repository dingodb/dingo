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

import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.operator.params.HashJoinParam;
import io.dingodb.exec.tuple.TupleKey;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class HashJoinOperator extends SoleOutOperator {
    public static final HashJoinOperator INSTANCE = new HashJoinOperator();

    private HashJoinOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        HashJoinParam param = vertex.getParam();
        TupleMapping leftMapping = param.getLeftMapping();
        TupleMapping rightMapping = param.getRightMapping();
        int leftLength = param.getLeftLength();
        int rightLength = param.getRightLength();
        boolean leftRequired = param.isLeftRequired();
        int pin = context.getPin();
        param.setContext(context);
        if (pin == 0) { // left
            waitRightFinFlag(param);
            TupleKey leftKey = new TupleKey(leftMapping.revMap(tuple));
            List<TupleWithJoinFlag> rightList = param.getHashMap().get(leftKey);
            if (rightList != null) {
                for (TupleWithJoinFlag t : rightList) {
                    Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                    System.arraycopy(t.getTuple(), 0, newTuple, leftLength, rightLength);
                    t.setJoined(true);
                    if (!edge.transformToNext(context, newTuple)) {
                        return false;
                    }
                }
            } else if (leftRequired) {
                Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                return edge.transformToNext(context, newTuple);
            }
        } else if (pin == 1) { //right
            TupleKey rightKey = new TupleKey(rightMapping.revMap(tuple));
            List<TupleWithJoinFlag> list = param.getHashMap()
                .computeIfAbsent(rightKey, k -> Collections.synchronizedList(new LinkedList<>()));
            list.add(new TupleWithJoinFlag(tuple));
        }
        return true;
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        if (fin instanceof FinWithException) {
            edge.fin(fin);
            return;
        }
        HashJoinParam param = vertex.getParam();
        boolean rightRequired = param.isRightRequired();
        int leftLength = param.getLeftLength();
        int rightLength = param.getRightLength();
        if (pin == 0) { // left
            if (rightRequired) {
                // should wait in case of no data push to left.
                waitRightFinFlag(param);
                outer:
                for (List<TupleWithJoinFlag> tList : param.getHashMap().values()) {
                    for (TupleWithJoinFlag t : tList) {
                        if (!t.isJoined()) {
                            Object[] newTuple = new Object[leftLength + rightLength];
                            Arrays.fill(newTuple, 0, leftLength, null);
                            System.arraycopy(t.getTuple(), 0, newTuple, leftLength, rightLength);
                            if (!edge.transformToNext(param.getContext(), newTuple)) {
                                break outer;
                            }
                        }
                    }
                }
            }
            edge.fin(fin);
            // Reset
            param.clear();
        } else if (pin == 1) { //right
            param.setRightFinFlag(true);
            param.getFuture().complete(null);
        }
    }

    private void waitRightFinFlag(HashJoinParam param) {
        param.getFuture().join();
        if (!param.isRightFinFlag()) {
            throw new RuntimeException();
        }
    }
}
