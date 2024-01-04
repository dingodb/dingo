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

package io.dingodb.proxy.model.dto;

import io.dingodb.sdk.service.entity.common.ScalarValue;
import io.dingodb.sdk.service.entity.common.Vector;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Collections;
import java.util.Map;

@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class VectorWithId {

    private long id;
    private Vector vector = Vector.builder()
        .binaryValues(Collections.emptyList())
        .floatValues(Collections.emptyList())
        .build();
    private Map<String, ScalarValue> scalarData = Collections.emptyMap();

    public void setId(long id) {
        this.id = id;
    }

    public void setVector(Vector vector) {
        if (vector == null) {
            return;
        }
        this.vector = vector;
    }

    public void setScalarData(Map<String, ScalarValue> scalarData) {
        if (scalarData == null) {
            return;
        }
        this.scalarData = scalarData;
    }
}
