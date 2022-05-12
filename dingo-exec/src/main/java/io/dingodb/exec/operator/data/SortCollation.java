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

package io.dingodb.exec.operator.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Comparator;

@Data
public class SortCollation {
    @JsonProperty("index")
    private final int index;
    @JsonProperty("direction")
    private final SortDirection direction;
    @JsonProperty("nullDirection")
    private final SortNullDirection nullDirection;

    @SuppressWarnings("unchecked")
    public Comparator<Object[]> makeComparator() {
        Comparator<Comparable<Object>> c = direction == SortDirection.DESCENDING
            ? Comparator.reverseOrder()
            : Comparator.naturalOrder();
        if (nullDirection != SortNullDirection.UNSPECIFIED) {
            c = (nullDirection == SortNullDirection.FIRST)
                ? Comparator.nullsFirst(c)
                : Comparator.nullsLast(c);
        }
        return Comparator.comparing((Object[] tuple) -> (Comparable<Object>) tuple[index], c);
    }
}
