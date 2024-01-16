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

package io.dingodb.exec.expr;

import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.operator.data.Content;
import io.dingodb.expr.rel.CacheOp;
import io.dingodb.expr.rel.PipeOp;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class RelOpUtils {
    private RelOpUtils() {
    }

    public static boolean processWithPipeOp(@NonNull PipeOp op, Object[] tuple, Edge edge, Content content) {
        Object[] out = op.put(tuple);
        if (out != null) {
            return edge.transformToNext(content, out);
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

    private static class BreakException extends RuntimeException {
        private static final long serialVersionUID = 2756240103043966868L;

        public BreakException(String msg) {
            super(msg);
        }
    }
}
