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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.SortCollation;
import lombok.Getter;
import lombok.NonNull;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

@Getter
@JsonTypeName("sort")
@JsonPropertyOrder({"collations", "limit", "offset"})
public class SortParam extends AbstractParams {

    @JsonProperty("collations")
    private final List<SortCollation> collations;
    @JsonProperty("limit")
    private final int limit;
    @JsonProperty("offset")
    private final int offset;
    private final List<Object[]> cache;
    private transient Comparator<Object[]> comparator;

    @JsonCreator
    public SortParam(
        @JsonProperty("collations") @NonNull List<SortCollation> collations,
        @JsonProperty("limit") int limit,
        @JsonProperty("offset") int offset
    ) {
        this.collations = collations;
        this.limit = limit;
        this.offset = offset;
        this.cache = new LinkedList<>();
        if (!collations.isEmpty()) {
            Comparator<Object[]> c = collations.get(0).makeComparator();
            for (int i = 1; i < collations.size(); ++i) {
                c = c.thenComparing(collations.get(i).makeComparator());
            }
            comparator = c;
        } else {
            comparator = null;
        }
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        if (!collations.isEmpty()) {
            Comparator<Object[]> c = collations.get(0).makeComparator();
            for (int i = 1; i < collations.size(); ++i) {
                c = c.thenComparing(collations.get(i).makeComparator());
            }
            comparator = c;
        } else {
            comparator = null;
        }
    }

    public void clear() {
        cache.clear();
    }
}
