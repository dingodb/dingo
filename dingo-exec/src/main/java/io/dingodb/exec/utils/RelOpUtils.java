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

package io.dingodb.exec.utils;

import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.util.Pair;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.ScanParam;
import io.dingodb.exec.operator.params.ScanWithRelOpParam;
import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.PipeOp;
import io.dingodb.store.api.transaction.DingoTransformedIterator;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

@Slf4j
public final class RelOpUtils {
    private RelOpUtils() {
    }

    public static boolean processWithPipeOp(@NonNull PipeOp op, Object[] tuple, Edge edge, Context context) {
        Object[] out = op.put(tuple);
        if (out != null) {
            return edge.transformToNext(context, out);
        }
        return true;
    }

    public static void forwardCacheOpResults(@NonNull CacheOp op, Edge edge) {
        try {
            op.get().forEach(tuple -> {
                if (!edge.transformToNext(tuple)) {
                    throw new BreakException("No more.");
                }
            });
        } catch (BreakException ignored) {
        }
    }

    public static Pair<Long, Boolean> doScan(
        Context context,
        @NonNull Vertex vertex,
        @NonNull Iterator<Object[]> iterator
    ) {
        ScanParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("scan");
        long count = 0;
        long tmp = System.currentTimeMillis();
        boolean breakFlg = false;
        while (iterator.hasNext()) {
            profile.time(tmp);
            Object[] tuple = iterator.next();
            if (!vertex.getSoleEdge().transformToNext(context, tuple)) {
                breakFlg = true;
                break;
            }
            tmp = System.currentTimeMillis();
        }
        if (iterator instanceof DingoTransformedIterator) {
            DingoTransformedIterator transformedIterator = (DingoTransformedIterator) iterator;
            OperatorProfile profile1 = (OperatorProfile) transformedIterator.getProfile();
            if (profile1 != null) {
                profile1.end();
                profile.getChildren().add(profile1);
            }
        }
        profile.time(tmp);
        profile.decreaseCount();
        profile.end();
        return Pair.of(count, !breakFlg);
    }

    public static Pair<Long, Boolean> doScanWithPipeOp(
        Context context,
        @NonNull Vertex vertex,
        @NonNull Iterator<Object[]> sourceIterator
    ) {
        ScanParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("doScanWithPipeOp");
        PipeOp relOp = (PipeOp) ((ScanWithRelOpParam) vertex.getParam()).getRelOp();
        Edge edge = vertex.getSoleEdge();
        long count = 0;
        long tmp = System.currentTimeMillis();
        boolean breakFlg = false;
        while (sourceIterator.hasNext()) {
            profile.time(tmp);
            Object[] tuple = sourceIterator.next();
            ++count;
            if (!processWithPipeOp(relOp, tuple, edge, context)) {
                breakFlg = true;
                break;
            }
            tmp = System.currentTimeMillis();
        }
        if (sourceIterator instanceof DingoTransformedIterator) {
            DingoTransformedIterator transformedIterator = (DingoTransformedIterator) sourceIterator;
            OperatorProfile profile1 = (OperatorProfile) transformedIterator.getProfile();
            if (profile1 != null) {
                profile1.end();
                profile.getChildren().add(profile1);
            }
        }
        profile.time(tmp);
        profile.decreaseCount();
        return Pair.of(count, !breakFlg);
    }

    public static Pair<Long, Boolean> doScanWithCacheOp(
        Context ignoredContext,
        @NonNull Vertex vertex,
        @NonNull Iterator<Object[]> sourceIterator
    ) {
        ScanParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("doScanWithCacheOp");
        CacheOp relOp = (CacheOp) ((ScanWithRelOpParam) param).getRelOp();
        long count = 0;
        long tmp = System.currentTimeMillis();
        boolean breakFlg = true;
        while (sourceIterator.hasNext()) {
            profile.time(tmp);
            Object[] tuple = sourceIterator.next();
            ++count;
            synchronized (relOp) {
                relOp.put(tuple);
            }
            tmp = System.currentTimeMillis();
        }
        if (sourceIterator instanceof DingoTransformedIterator) {
            DingoTransformedIterator transformedIterator = (DingoTransformedIterator) sourceIterator;
            OperatorProfile profile1 = (OperatorProfile) transformedIterator.getProfile();
            if (profile1 != null) {
                profile1.end();
                profile.getChildren().add(profile1);
            }
        }
        profile.time(tmp);
        profile.decreaseCount();

        synchronized (relOp) {
            forwardCacheOpResults(relOp, vertex.getSoleEdge());
            relOp.clear();
        }
        return Pair.of(count, breakFlg);
    }

    private static class BreakException extends RuntimeException {
        private static final long serialVersionUID = 2756240103043966868L;

        public BreakException(String msg) {
            super(msg);
        }
    }
}
