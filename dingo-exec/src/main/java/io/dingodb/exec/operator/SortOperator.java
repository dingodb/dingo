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

package io.dingodb.exec.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.exec.fin.Fin;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;

@JsonTypeName("sort")
@JsonPropertyOrder({"collations", "limit", "offset", "output"})
public class SortOperator extends SoleOutOperator {
    @JsonProperty("collations")
    private final List<SortCollation> collations;
    @JsonProperty("limit")
    private final int limit;
    @JsonProperty("offset")
    private final int offset;

    private final List<Object[]> cache;
    private final Comparator<Object[]> comparator;

    @JsonCreator
    public SortOperator(
        @Nonnull @JsonProperty("collations") List<SortCollation> collations,
        @JsonProperty("limit") int limit,
        @JsonProperty("offset") int offset
    ) {
        this.limit = limit;
        this.offset = offset;
        this.collations = collations;
        this.cache = new LinkedList<>();
        if (!collations.isEmpty()) {
            Comparator<Object[]> c = makeComparator(collations.get(0));
            for (int i = 1; i < collations.size(); ++i) {
                c = c.thenComparing(makeComparator(collations.get(i)));
            }
            comparator = c;
        } else {
            comparator = null;
        }
    }

    @SuppressWarnings("unchecked")
    private static Comparator<Object[]> makeComparator(@Nonnull SortCollation collation) {
        Comparator<Comparable<Object>> c = collation.getDirection().isDescending()
            ? Comparator.reverseOrder()
            : Comparator.naturalOrder();
        c = (collation.getNullDirection() == RelFieldCollation.NullDirection.FIRST)
            ? Comparator.nullsFirst(c)
            : Comparator.nullsLast(c);
        return Comparator.comparing((Object[] tuple) -> (Comparable<Object>) tuple[collation.getIndex()], c);
    }

    @Override
    public boolean push(int pin, Object[] tuple) {
        cache.add(tuple);
        return collations.size() > 0 || limit <= 0 || cache.size() < offset + limit;
    }

    @Override
    public void fin(int pin, Fin fin) {
        if (comparator != null) {
            cache.sort(comparator);
        }
        int o = 0;
        int c = 0;
        for (Object[] tuple : cache) {
            if (o < offset) {
                ++o;
                continue;
            }
            if (limit > 0 && c >= limit) {
                break;
            }
            if (!output.push(tuple)) {
                break;
            }
            ++c;
        }
        output.fin(fin);
    }
}
