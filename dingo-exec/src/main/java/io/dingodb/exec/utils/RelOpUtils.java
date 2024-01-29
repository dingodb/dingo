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

import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.ScanWithRelOpParam;
import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.PipeOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

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

    public static long doScan(
        Context context,
        @NonNull Vertex vertex,
        @NonNull Iterator<Object[]> iterator
    ) {
        long count = 0;
        while (iterator.hasNext()) {
            Object[] tuple = iterator.next();
            ++count;
            if (!vertex.getSoleEdge().transformToNext(context, tuple)) {
                break;
            }
        }
        return count;
    }

    public static long doScanWithPipeOp(
        Context context,
        @NonNull Vertex vertex,
        @NonNull Iterator<Object[]> sourceIterator
    ) {
        PipeOp relOp = (PipeOp) ((ScanWithRelOpParam) vertex.getParam()).getRelOp();
        Edge edge = vertex.getSoleEdge();
        long count = 0;
        while (sourceIterator.hasNext()) {
            Object[] tuple = sourceIterator.next();
            ++count;
            if (!processWithPipeOp(relOp, tuple, edge, context)) {
                break;
            }
        }
        return count;
    }

    public static long doScanWithCacheOp(
        Context ignoredContext,
        @NonNull Vertex vertex,
        @NonNull Iterator<Object[]> sourceIterator
    ) {
        CacheOp relOp = (CacheOp) ((ScanWithRelOpParam) vertex.getParam()).getRelOp();
        long count = 0;
        while (sourceIterator.hasNext()) {
            Object[] tuple = sourceIterator.next();
            ++count;
            relOp.put(tuple);
        }
        forwardCacheOpResults(relOp, vertex.getSoleEdge());
        relOp.clear();
        return count;
    }

    private static class BreakException extends RuntimeException {
        private static final long serialVersionUID = 2756240103043966868L;

        public BreakException(String msg) {
            super(msg);
        }
    }
}
