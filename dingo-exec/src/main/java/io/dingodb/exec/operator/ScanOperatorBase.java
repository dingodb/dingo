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

import io.dingodb.common.util.Pair;
import io.dingodb.exec.base.Status;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.exception.TaskCancelException;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.ScanParam;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class ScanOperatorBase extends SoleOutOperator {
    protected abstract @NonNull Iterator<Object[]> createIterator(@NonNull Context context, @NonNull Vertex vertex);

    protected abstract @NonNull Scanner getScanner(@NonNull Context context, @NonNull Vertex vertex);

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        if(vertex.getTask().getStatus() == Status.CANCEL) {
            throw new TaskCancelException("task is cancel");
        } else if (vertex.getTask().getStatus() == Status.STOPPED) {
            return false;
        }
        Iterator<Object[]> iterator = createIterator(context, vertex);
        Pair<Long, Boolean> res = getScanner(context, vertex).apply(context, vertex, iterator);
        // Scan operator is not source operator, so may be push multiple times.
        return res.getValue();
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, @NonNull Vertex vertex) {
        if (fin instanceof FinWithProfiles) {
            ScanParam param = vertex.getParam();
            FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
            finWithProfiles.addProfileList(param.getProfileList());
        }
        vertex.getSoleEdge().fin(fin);
    }

    public interface Scanner {
        Pair<Long, Boolean> apply(@NonNull Context context, @NonNull Vertex vertex, @NonNull Iterator<Object[]> iterator);
    }
}
