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

package io.dingodb.exec.aggregate;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class GroupConcatContext {

    /**
     * All accumulated rows. Each row contains concat-value columns followed by order-key columns.
     */
    @Getter
    private final List<Object[]> rows;

    /**
     * Whether DISTINCT is active. Uses a LinkedHashSet to track seen string keys.
     */
    private final boolean distinct;
    private final Set<String> seen;  // only non-null when distinct=true

    public GroupConcatContext(boolean distinct) {
        this.distinct = distinct;
        this.rows = new ArrayList<>();
        this.seen = distinct ? new LinkedHashSet<>() : null;
    }

    /**
     * Try to add a row. If DISTINCT is on and the key is already seen, the row is skipped.
     *
     * @param row the row to add (concat values + order-key values)
     * @return true if added, false if skipped due to DISTINCT
     */
    public boolean addRow(Object[] row) {
        if (distinct) {
            // Build a string key from the concat-value part only
            String key = buildDistinctKey(row);
            if (!seen.add(key)) {
                return false;
            }
        }
        rows.add(row);
        return true;
    }

    /**
     * Merge another context into this one (used in reduce phase).
     */
    public void merge(GroupConcatContext other) {
        for (Object[] row : other.rows) {
            addRow(row);
        }
    }

    private String buildDistinctKey(Object[] row) {
        // Only use the first element (the expr value) as the distinct key
        return row[0] == null ? "\0NULL\0" : row[0].toString();
    }

    @Override
    public String toString() {
        return "GroupConcatContext{rows=" + rows.size() + ", distinct=" + distinct + "}";
    }
}
