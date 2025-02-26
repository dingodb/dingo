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

import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.profile.Profile;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.operator.params.HashJoinParam;
import io.dingodb.exec.tuple.TupleKey;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class HashJoinOperator extends SoleOutOperator {
    public static final HashJoinOperator INSTANCE = new HashJoinOperator();

    private HashJoinOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        HashJoinParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("hashJoin");
        long start = System.currentTimeMillis();
        try {
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
                boolean isEmpty = isEmpty(leftKey, param);
                if (isEmpty && ("inner".equalsIgnoreCase(param.getJoinType()) || "right".equalsIgnoreCase(param.getJoinType()))) {
                    return true;
                }
                if (isEmpty && "left".equalsIgnoreCase(param.getJoinType())) {
                    Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                    Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                    return pushToNext(param, edge, context, newTuple);
                }
                List<TupleWithJoinFlag> rightList = param.getHashMap().get(leftKey);
                if (rightList != null) {
                    for (TupleWithJoinFlag t : rightList) {
                        Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                        System.arraycopy(t.getTuple(), 0, newTuple, leftLength, rightLength);
                        t.setJoined(true);
                        if (!pushToNext(param, edge, context, newTuple)) {
                            return false;
                        }
                    }
                } else if (leftRequired) {
                    Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                    Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                    return pushToNext(param, edge, context, newTuple);
                }
            } else if (pin == 1) { //right
                TupleKey rightKey = new TupleKey(rightMapping.revMap(tuple));
                if (isEmpty(rightKey, param) && "inner".equalsIgnoreCase(param.getJoinType())) {
                    return true;
                }
                List<TupleWithJoinFlag> list = param.getHashMap()
                    .computeIfAbsent(rightKey, k -> Collections.synchronizedList(new LinkedList<>()));
                list.add(new TupleWithJoinFlag(tuple));
            }
            return true;
        } finally {
            profile.time(start);
        }
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
                            if (!pushToNext(param, edge, param.getContext(), newTuple)) {
                                break outer;
                            }
                        }
                    }
                }
            }
            if (fin instanceof FinWithProfiles) {
                FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                param.setProfileLeft(finWithProfiles.getProfile());
                Profile profile = param.getProfile();
                if (profile == null) {
                    profile = param.getProfile("hashJoin");
                }
                profile.getChildren().add(param.profileLeft);
                //if (param.getProfileRight() == null) {
                //    waitRightFinFlag(param);
                //}
                profile.getChildren().add(param.profileRight);
                profile.end();
                finWithProfiles.setProfile(profile);
            }
            edge.fin(fin);
            // Reset
            param.clear();
        } else if (pin == 1) { //right
            if (fin instanceof FinWithProfiles) {
                FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                param.setProfileRight(finWithProfiles.getProfile());
            }
            param.setRightFinFlag(true);
            param.getFuture().complete(null);
        }
    }

    private static void waitRightFinFlag(HashJoinParam param) {
        param.getFuture().join();
        if (!param.isRightFinFlag()) {
            throw new RuntimeException();
        }
    }

    private static boolean pushToNext(HashJoinParam param, Edge edge, Context context, Object[] tuple) {
        if (param.getOtherExpr() != null) {
            Object object = param.getOtherExpr().eval(tuple);
            if (object != null && (Boolean) object) {
                return edge.transformToNext(context, tuple);
            } else {
                return true;
            }
        } else {
            return edge.transformToNext(context, tuple);
        }
    }

    public static boolean isEmpty(TupleKey tupleKey, HashJoinParam param) {
        if (param.rightMappingEmpty && param.leftMappingEmpty) {
            return false;
        }
        Object[] tuple = tupleKey.getTuple();
        if (tuple != null) {
            for (Object item : tuple) {
                if (item != null) {
                    return false;
                }
            }
        }
        return true;
    }
}
