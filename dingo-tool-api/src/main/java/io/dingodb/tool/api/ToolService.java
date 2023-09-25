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

package io.dingodb.tool.api;

import io.dingodb.common.CommonId;

import java.util.List;

public interface ToolService {

    static ToolService getDefault() {
        return ToolServiceProvider.getDefault().get();
    }

    public List<List<Float>> vectorCalcDistance(CommonId indexId,
                                                CommonId regionId,
                                                io.dingodb.common.vector.VectorCalcDistance vectorCalcDistance);
}
