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

package io.dingodb.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.Location;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class Part implements Serializable {

    private static final long serialVersionUID = -2767354268752865267L;

    @JsonProperty("id")
    private byte[] id;

    @JsonProperty("leader")
    private Location leader;

    @JsonProperty("locations")
    private List<Location> replicates;

    @JsonCreator
    public Part(
        @JsonProperty("leader") byte[] id,
        @JsonProperty("leader") Location leader,
        @JsonProperty("locations") List<Location> replicates
    ) {
        this.id = id;
        this.leader = leader;
        this.replicates = replicates;
    }

}
