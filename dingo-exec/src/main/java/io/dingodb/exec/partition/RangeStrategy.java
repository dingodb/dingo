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

package io.dingodb.exec.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.codec.AvroCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;

import java.io.IOException;
import java.util.NavigableSet;
import javax.annotation.Nonnull;

@JsonPropertyOrder({"definition", "ranges"})
@JsonTypeName("RangeHash")
public class RangeStrategy extends PartitionStrategy<ComparableByteArray> {

    @JsonProperty("definition")
    private final TableDefinition definition;
    private final transient AvroCodec codec;

    @JsonProperty("ranges")
    private final NavigableSet<ComparableByteArray> ranges;

    @JsonCreator
    public RangeStrategy(
        @JsonProperty("definition") TableDefinition definition,
        @JsonProperty("ranges") NavigableSet<ComparableByteArray> ranges
    ) {
        this.ranges = ranges;
        this.definition = definition;
        this.codec = new AvroCodec(definition.getAvroSchemaOfKey());
    }

    @Override
    public int getPartNum() {
        return ranges.size();
    }

    @Override
    public ComparableByteArray calcPartId(@Nonnull Object[] keyTuple) {
        try {
            return ranges.floor(new ComparableByteArray(codec.encode(keyTuple)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
