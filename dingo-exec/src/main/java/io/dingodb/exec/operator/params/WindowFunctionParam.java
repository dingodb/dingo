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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.tool.api.WindowService;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@JsonTypeName("window")
@JsonPropertyOrder({"funName"})
public class WindowFunctionParam extends AbstractParams {

    @JsonProperty("funName")
    String funName;

    @Getter
    List<Object[]> list = new ArrayList<>();

    @Getter
    WindowService windowService;

    public WindowFunctionParam(WindowService windowService) {
        this.windowService = windowService;
    }
}
