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
import io.dingodb.exec.operator.params.PartitionParam;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class PartitionOperator extends FanOutOperator {
    public static final PartitionOperator INSTANCE = new PartitionOperator();

    private PartitionOperator() {

    }

    @Override
    protected int calcOutputIndex(Context context, Object @NonNull [] tuple, Vertex vertex) {
        PartitionParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("partition");
        long start = System.currentTimeMillis();
        int index = param.getPartIndices().get(context.getDistribution().getId().domain);
        profile.time(start);
        return index;
    }

}
