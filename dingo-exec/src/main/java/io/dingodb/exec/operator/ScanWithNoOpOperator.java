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

import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ScanWithNoOpOperator extends ScanWithRelOpOperator {
    public static ScanWithNoOpOperator INSTANCE = new ScanWithNoOpOperator();

    @Override
    protected long doPush(Context context, @NonNull Vertex vertex, @NonNull Iterator<Object[]> sourceIterator) {
        long count = 0;
        while (sourceIterator.hasNext()) {
            Object[] tuple = sourceIterator.next();
            ++count;
            if (!vertex.getSoleEdge().transformToNext(context, tuple)) {
                break;
            }
        }
        return count;
    }
}
