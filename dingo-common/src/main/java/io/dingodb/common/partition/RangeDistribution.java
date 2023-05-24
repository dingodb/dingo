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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class RangeDistribution implements Distribution {

    private static final long serialVersionUID = -2767354268752865267L;

    @JsonProperty("id")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private CommonId id;

    @JsonProperty("startKey")
    private byte[] startKey;

    @JsonProperty("endKey")
    private byte[] endKey;

    @JsonProperty("withStart")
    private boolean withStart = true;

    @JsonProperty("withEnd")
    private boolean withEnd = false;

    public RangeDistribution(@JsonProperty("id") CommonId id) {
        this.id = id;
    }

    public RangeDistribution(CommonId id, byte[] startKey, byte[] endKey) {
        this.id = id;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    @JsonCreator
    public RangeDistribution(
        @JsonProperty("id") CommonId id,
        @JsonProperty("startKey") byte[] startKey,
        @JsonProperty("endKey") byte[] endKey,
        @JsonProperty("withStart") boolean withStart,
        @JsonProperty("withEnd") boolean withEnd
    ) {
        this.id = id;
        this.startKey = startKey;
        this.endKey = endKey;
        this.withStart = withStart;
        this.withEnd = withEnd;
    }

    @Override
    public CommonId id() {
        return id;
    }
}
