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

import io.dingodb.sdk.service.entity.common.ScalarValue;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class VectorScanQuery {

    private Long startId;
    private Boolean isReverseScan;
    private Long maxScanCount;
    private Long endId;

    private Boolean withoutVectorData;
    private Boolean withoutScalarData;
    private List<String> selectedKeys;
    private Boolean withoutTableData;

    // Whether to use scalar filtering.
    private Boolean useScalarFilter;
    private Map<String, ScalarValue> scalarForFilter;

}
