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

package io.dingodb.common.partition;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@ToString
@AllArgsConstructor
public class PartitionDefinition implements Serializable {

    private static final long serialVersionUID = 2252446672472101114L;

    @JsonProperty("funcName")
    String funcName;

    @JsonProperty("columns")
    List<String> columns;

    @JsonProperty("details")
    List<PartitionDetailDefinition> details;

    public PartitionDefinition(String funcName, List<String> columns) {
        this.funcName = funcName;
        this.columns = columns;
    }

    public PartitionDefinition() {
    }

}
