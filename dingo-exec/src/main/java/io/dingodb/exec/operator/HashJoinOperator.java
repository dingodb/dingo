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

import io.dingodb.common.log.LogUtils;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.profile.Profile;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.operator.params.HashJoinParam;
import io.dingodb.exec.tuple.TupleKey;
import io.dingodb.expr.rel.PipeOp;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
public class HashJoinOperator extends SoleOutOperator {
    public static final HashJoinOperator INSTANCE = new HashJoinOperator();

    private HashJoinOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        HashJoinParam param = vertex.getParam();
        try {
            TupleMapping leftMapping = param.getLeftMapping();
            TupleMapping rightMapping = param.getRightMapping();
            int leftLength = param.getLeftLength();
            int rightLength = param.getRightLength();
            boolean leftRequired = param.isLeftRequired();
            int pin = context.getPin();
            param.setContext(context);
            if (pin == 0) { // left
                waitRightFinFlag(param, vertex);
                OperatorProfile profile = param.getProfile("hashJoin");
                long start = System.currentTimeMillis();
                TupleKey leftKey = HashJoinParam.rtrimTupleKey(new TupleKey(leftMapping.revMap(tuple)));
                if (HashJoinParam.containsNull(leftKey)) {
                    if ("inner".equalsIgnoreCase(param.getJoinType()) || "right".equalsIgnoreCase(param.getJoinType())) {
                        return true;
                    } else if ("left".equalsIgnoreCase(param.getJoinType()) || "full".equalsIgnoreCase(param.getJoinType())) {
                        Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                        Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                        return pushToNext(param, edge, context, newTuple);
                    }
                }
                boolean isEmpty = isEmpty(leftKey, param);
                if (isEmpty && ("inner".equalsIgnoreCase(param.getJoinType())
                    || "right".equalsIgnoreCase(param.getJoinType()))) {
                    profile.opTime(start);
                    return true;
                }
                if (isEmpty && "left".equalsIgnoreCase(param.getJoinType())) {
                    Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                    Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                    profile.opTime(start);
                    return pushToNext(param, edge, context, newTuple);
                }
                List<TupleWithJoinFlag> rightList = param.getHashMap().get(leftKey);
                if (rightList != null) {
                    for (TupleWithJoinFlag t : rightList) {
                        Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                        System.arraycopy(t.getTuple(), 0, newTuple, leftLength, rightLength);
                        t.setJoined(true);
                        profile.opTime(start);
                        if (!pushToNext(param, edge, context, newTuple)) {
                            return false;
                        }
                    }
                } else if (leftRequired) {
                    Object[] newTuple = Arrays.copyOf(tuple, leftLength + rightLength);
                    Arrays.fill(newTuple, leftLength, leftLength + rightLength, null);
                    profile.opTime(start);
                    return pushToNext(param, edge, context, newTuple);
                }
            } else if (pin == 1) { //right
                OperatorProfile profile = param.getProfile("hashJoin");
                long start = System.currentTimeMillis();
                TupleKey rightKey = HashJoinParam.rtrimTupleKey(new TupleKey(rightMapping.revMap(tuple)));
                if (HashJoinParam.containsNull(rightKey)) {
                    if ("inner".equalsIgnoreCase(param.getJoinType()) || "left".equalsIgnoreCase(param.getJoinType())) {
                        return true;
                    } else if ("right".equalsIgnoreCase(param.getJoinType()) || "full".equalsIgnoreCase(param.getJoinType())) {
                        List<TupleWithJoinFlag> list = param.getHashMap()
                            .computeIfAbsent(rightKey, k -> Collections.synchronizedList(new LinkedList<>()));
                        list.add(new TupleWithJoinFlag(tuple));
                    }
                } else {
                    if (isEmpty(rightKey, param) && "inner".equalsIgnoreCase(param.getJoinType())) {
                        profile.cacheOpTime(start);
                        return true;
                    }
                    List<TupleWithJoinFlag> list = param.getHashMap()
                        .computeIfAbsent(rightKey, k -> Collections.synchronizedList(new LinkedList<>()));
                    list.add(new TupleWithJoinFlag(tuple));
                }
                profile.cacheOpTime(start);
            }
            return true;
        } finally {
            if (vertex.getTask().getStatus() == Status.STOPPED || vertex.getTask().getStatus() == Status.CANCEL) {
                LogUtils.warn(log, "Task status is {} ...", vertex.getTask().getStatus());
                param.interrupt();
            }
        }
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        Edge edge = vertex.getSoleEdge();
        HashJoinParam param = vertex.getParam();
        if (vertex.getTask().getStatus() == Status.STOPPED || vertex.getTask().getStatus() == Status.CANCEL) {
            LogUtils.warn(log, "Task status is {} ...", vertex.getTask().getStatus());
            param.interrupt();
        }
        if (fin instanceof FinWithException) {
            param.interrupt();
            edge.fin(fin);
            return;
        }
        boolean rightRequired = param.isRightRequired();
        int leftLength = param.getLeftLength();
        int rightLength = param.getRightLength();
        if (pin == 0) { // left
            if (rightRequired) {
                // should wait in case of no data push to left.
                waitRightFinFlag(param, vertex);
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
            if (vertex.getTask().getStatus() == Status.STOPPED || vertex.getTask().getStatus() == Status.CANCEL) {
                LogUtils.warn(log, "Task status is {} ...", vertex.getTask().getStatus());
                param.interrupt();
            }
        } else if (pin == 1) { //right
            if (fin instanceof FinWithProfiles) {
                FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                param.setProfileRight(finWithProfiles.getProfile());
            }
            param.setRightFinFlag(true);
            param.getFuture().complete(null);
        }
    }

    private static void waitRightFinFlag(HashJoinParam param, Vertex vertex) {
        if (param.isInterrupted()) {
            throw new RuntimeException("HashJoin operation interrupted before waiting");
        }
        if (vertex.getTask().getStatus() == Status.STOPPED || vertex.getTask().getStatus() == Status.CANCEL) {
            LogUtils.warn(log, "Task status is {} ...", vertex.getTask().getStatus());
            param.interrupt();
            throw new RuntimeException("task is cancel");
        }
        try {
            param.getFuture().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            param.interrupt();
            throw new RuntimeException("Wait for right side completion interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Error while waiting for right side completion", e);
        }
        if (param.isInterrupted()) {
            throw new RuntimeException("HashJoin operation interrupted after waiting");
        }

        if (!param.isRightFinFlag()) {
            throw new RuntimeException("Right fin flag not set after future completed");
        }
    }

    private static boolean pushToNext(HashJoinParam param, Edge edge, Context context, Object[] tuple) {
        Object[] tmpTuple = param.rtrimTuple(tuple);
        if (param.getOtherExpr() != null) {
            Object object;
            synchronized (param.getOtherExpr()) {
                object = param.getOtherExpr().eval(tmpTuple);
            }
            if (object != null && (Boolean) object) {
                return edge.transformToNext(context, tuple);
            } else {
                return true;
            }
        } else {
            if (param.getRelOp() != null) {
                Object object;
                synchronized (param.getRelOp()) {
                    object = ((Object[]) ((PipeOp) param.getRelOp()).put(tmpTuple))[0];
                }
                if (object != null && (Boolean) object) {
                    return edge.transformToNext(context, tuple);
                } else {
                    return true;
                }
            }
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
