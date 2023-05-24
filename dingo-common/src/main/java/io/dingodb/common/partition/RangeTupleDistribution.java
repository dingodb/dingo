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
import io.dingodb.common.CommonId;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class RangeTupleDistribution implements Distribution {

    private static final long serialVersionUID = -2767354268752865267L;

    @JsonProperty("id")
    private CommonId id;

    @JsonProperty("startKey")
    private byte[] startKey;

    @JsonProperty("start")
    private Object[] start;

    @JsonProperty("end")
    private Object[] end;

    @JsonCreator
    public RangeTupleDistribution(@JsonProperty("id") CommonId id) {
        this.id = id;
    }

    @JsonCreator
    public RangeTupleDistribution(
        @JsonProperty("id") CommonId id,
        @JsonProperty("startKey") byte[] startKey,
        @JsonProperty("start") Object[] start,
        @JsonProperty("end") Object[] end
    ) {
        this.id = id;
        this.startKey = startKey;
        this.start = start;
        this.end = end;
    }

    @Override
    public CommonId id() {
        return id;
    }
}
