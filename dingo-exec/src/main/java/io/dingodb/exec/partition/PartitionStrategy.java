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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.exec.hash.SimpleHashStrategy;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(SimpleHashStrategy.class),
    @JsonSubTypes.Type(RangeStrategy.class),
})
public abstract class PartitionStrategy<I> {
    public abstract int getPartNum();

    // Should be `String` for json serialization.
    public abstract I calcPartId(@Nonnull final Object[] keyTuple);

    public I calcPartId(@Nonnull final Object[] tuple, @Nonnull TupleMapping keyMapping) {
        Object[] keyTuple = keyMapping.revMap(tuple);
        return calcPartId(keyTuple);
    }

    public Map<I, List<Object[]>> partKeyTuples(
        @Nonnull final Collection<Object[]> keyTuples
    ) {
        Map<I, List<Object[]>> map = new LinkedHashMap<>(getPartNum());
        for (Object[] tuple : keyTuples) {
            I partId = calcPartId(tuple);
            map.putIfAbsent(partId, new LinkedList<>());
            map.get(partId).add(tuple);
        }
        return map;
    }

    public Map<I, List<Object[]>> partTuples(
        @Nonnull final Collection<Object[]> tuples,
        @Nonnull TupleMapping keyMappings
    ) {
        Map<I, List<Object[]>> map = new LinkedHashMap<>(getPartNum());
        for (Object[] tuple : tuples) {
            I partId = calcPartId(tuple, keyMappings);
            map.putIfAbsent(partId, new LinkedList<>());
            map.get(partId).add(tuple);
        }
        return map;
    }
}
