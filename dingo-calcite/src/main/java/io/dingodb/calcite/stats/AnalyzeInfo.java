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

package io.dingodb.calcite.stats;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.NoArgsConstructor;

import java.util.List;

@NoArgsConstructor
public class AnalyzeInfo {

    @JsonProperty("cmSketchHeight")
    private Integer cmSketchHeight = 5;
    @JsonProperty("cmSketchWidth")
    private Integer cmSketchWidth = 10000;
    @JsonProperty("bucketCount")
    private Integer bucketCount = 254;
    @JsonProperty("columns")
    private List<String> columns;

    public AnalyzeInfo(Integer cmSketchHeight, Integer cmSketchWidth, Integer bucketCount, List<String> columns) {
        this.cmSketchHeight = cmSketchHeight;
        this.cmSketchWidth = cmSketchWidth;
        this.bucketCount = bucketCount;
        this.columns = columns;
    }
}
