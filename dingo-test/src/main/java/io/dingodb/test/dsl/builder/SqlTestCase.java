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

package io.dingodb.test.dsl.builder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.test.dsl.builder.step.Step;
import lombok.Getter;
import lombok.Setter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class SqlTestCase {
    @Getter
    private final String name;
    @Getter
    private final List<Step> steps;
    // The mapping from placeholder name to table id.
    @Getter
    private final Map<String, String> tableMapping;
    // The table ids that were modified in this test.
    @Getter
    private final List<String> modifiedTableIds;

    @Getter
    @Setter
    private boolean enabled;
    @Getter
    @Setter
    private boolean only;

    public SqlTestCase(@NonNull String name) {
        this.name = name;
        this.steps = new LinkedList<>();
        this.enabled = true;
        this.only = false;
        this.tableMapping = new HashMap<>(1);
        this.modifiedTableIds = new ArrayList<>(1);
    }

    @JsonCreator
    public SqlTestCase(
        @JsonProperty("name") @NonNull String name,
        @JsonProperty("steps") List<Step> steps,
        @JsonProperty(value = "skip", defaultValue = "false") boolean skipped,
        @JsonProperty(value = "only", defaultValue = "false") boolean only,
        @JsonProperty("use") Map<String, String> tableMapping,
        @JsonProperty("modify") List<String> modifiedTableIds
    ) {
        this.name = name;
        this.steps = steps;
        this.enabled = !skipped;
        this.only = only;
        this.tableMapping = tableMapping != null ? tableMapping : new HashMap<>(1);
        this.modifiedTableIds = modifiedTableIds != null ? modifiedTableIds : new ArrayList<>(1);
    }
}
