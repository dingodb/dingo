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

package io.dingodb.common.hash;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dingodb.common.table.TupleMapping;

import javax.annotation.Nonnull;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(SimpleHashStrategy.class),
})
public interface HashStrategy {
    // Should be `String` for json serialization.
    int calcHash(@Nonnull final Object[] tuple);

    default int calcHash(@Nonnull final Object[] tuple, @Nonnull TupleMapping mapping) {
        Object[] t = mapping.revMap(tuple);
        return calcHash(t);
    }
}
