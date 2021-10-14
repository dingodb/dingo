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

package io.dingodb.common.part;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Getter;

import java.util.Objects;
import javax.annotation.Nonnull;

@JsonTypeName("simpleHash")
public class SimpleHashStrategy extends AbstractPartStrategy {
    @JsonProperty("partNum")
    @Getter
    private final int partNum;

    @JsonCreator
    public SimpleHashStrategy(
        @JsonProperty("partNum") int partNum
    ) {
        this.partNum = partNum;
    }

    @Override
    public Object calcPartId(@Nonnull Object[] keyTuples) {
        int hash = Objects.hash(keyTuples);
        return Math.abs(hash) % partNum;
    }
}
