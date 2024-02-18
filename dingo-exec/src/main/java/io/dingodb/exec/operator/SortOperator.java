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

import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.operator.params.SortParam;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Comparator;
import java.util.List;

public class SortOperator extends SoleOutOperator {
    public static final SortOperator INSTANCE = new SortOperator();

    private SortOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            SortParam param = vertex.getParam();
            param.setContext(context);
            int limit = param.getLimit();
            int offset = param.getOffset();
            List<SortCollation> collations = param.getCollations();
            if (limit == 0) {
                return false;
            }
            param.getCache().add(tuple);
            return collations.size() > 0 || limit < 0 || param.getCache().size() < offset + limit;
        }
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        synchronized (vertex) {
            SortParam param = vertex.getParam();
            int limit = param.getLimit();
            int offset = param.getOffset();
            List<Object[]> cache = param.getCache();
            Comparator<Object[]> comparator = param.getComparator();
            if (comparator != null) {
                cache.sort(comparator);
            }
            int o = 0;
            int c = 0;
            Edge edge = vertex.getSoleEdge();
            for (Object[] tuple : cache) {
                if (o < offset) {
                    ++o;
                    continue;
                }
                if (limit >= 0 && c >= limit) {
                    break;
                }
                if (!edge.transformToNext(param.getContext(), tuple)) {
                    break;
                }
                ++c;
            }
            edge.fin(fin);
            // Reset
            param.clear();
        }
    }
}
