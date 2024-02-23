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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;

@Getter
@Setter
@Builder
@EqualsAndHashCode
public class RangeDistribution implements Distribution, Serializable {

    private static final long serialVersionUID = -2767354268752865267L;

    @JsonProperty("id")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private CommonId id;

    @JsonProperty("startKey")
    private byte[] startKey;

    @JsonProperty("endKey")
    private byte[] endKey;

    @JsonProperty("start")
    private Object[] start;

    @JsonProperty("end")
    private Object[] end;

    @Builder.Default
    @JsonProperty("withStart")
    private boolean withStart = true;

    @Builder.Default
    @JsonProperty("withEnd")
    private boolean withEnd = false;

    @JsonCreator
    public RangeDistribution(
        @JsonProperty("id") CommonId id,
        @JsonProperty("startKey") byte[] startKey,
        @JsonProperty("endKey") byte[] endKey,
        @JsonProperty("start") Object[] start,
        @JsonProperty("end") Object[] end,
        @JsonProperty("withStart") boolean withStart,
        @JsonProperty("withEnd") boolean withEnd
    ) {
        this.id = id;
        this.startKey = startKey;
        this.endKey = endKey;
        this.start = start;
        this.end = end;
        this.withStart = withStart;
        this.withEnd = withEnd;
    }

    @Override
    public CommonId id() {
        return id;
    }

    public byte[] getStartKey() {
        return startKey == null ? null : Arrays.copyOf(startKey, startKey.length);
    }

    public byte[] getEndKey() {
        return endKey == null ? null : Arrays.copyOf(endKey, endKey.length);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " "
            + id + ": "
            + (withStart ? '[' : '(')
            + Arrays.toString(startKey)
            + ", "
            + Arrays.toString(endKey)
            + (withEnd ? ']' : ')');
    }
}
