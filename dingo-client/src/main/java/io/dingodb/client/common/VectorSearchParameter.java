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

package io.dingodb.client.common;

import io.dingodb.sdk.common.vector.Search;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class VectorSearchParameter {

    private Integer topN;
    private boolean withoutVectorData;
    private boolean withoutScalarData;
    private List<String> selectedKeys;
    private boolean withoutTableData;
    private Search search;

    @Deprecated
    private boolean useScalarFilter;

    private io.dingodb.sdk.common.vector.VectorSearchParameter.VectorFilter vectorFilter;
    private io.dingodb.sdk.common.vector.VectorSearchParameter.VectorFilterType vectorFilterType;
    private VectorCoprocessor coprocessor;
    private List<Long> vectorIds;

    private boolean useBruteForce;
}
