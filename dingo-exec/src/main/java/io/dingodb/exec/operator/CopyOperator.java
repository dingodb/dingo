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
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.AbstractParams;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CopyOperator extends FanOutOperator {
    public static final CopyOperator INSTANCE = new CopyOperator();

    private CopyOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        AbstractParams param = vertex.getParam();
        OperatorProfile profile = param.getProfile("copy");
        long start = System.currentTimeMillis();
        vertex.getOutList().forEach(o -> o.transformToNext(context, tuple));
        profile.time(start);
        return true;
    }

    @Override
    protected int calcOutputIndex(Context context, Object @NonNull [] tuple, Vertex vertex) {
        return 0;
    }
}
