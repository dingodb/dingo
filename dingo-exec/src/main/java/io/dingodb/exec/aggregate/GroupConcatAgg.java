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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.AggregationOperator;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.meta.InfoSchemaService;
import org.apache.calcite.rel.RelFieldCollation;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@JsonTypeName("groupConcat")
public class GroupConcatAgg extends AbstractAgg {

    /** Column index of the expression to concatenate (primary expr). */
    @JsonProperty("index")
    private final int index;

    /**
     * Additional column indices used for ORDER BY that are appended after the
     * primary expression value when collecting rows. This allows sorting before
     * the final string is assembled.
     */
    @JsonProperty("orderByIndices")
    private final List<Integer> orderByIndices;

    /**
     * Sort directions for each ORDER BY column.
     * {@code true} = ASC, {@code false} = DESC.
     */
    @JsonProperty("orderByAscending")
    private final List<Boolean> orderByAscending;

    /** The string placed between concatenated values. Defaults to ",". */
    @JsonProperty("separator")
    private final String separator;

    /** Whether DISTINCT deduplication is active. */
    @JsonProperty("distinct")
    private final boolean distinct;

    @JsonCreator
    public GroupConcatAgg(
        @JsonProperty("index") int index,
        @JsonProperty("orderByIndices") List<Integer> orderByIndices,
        @JsonProperty("orderByAscending") List<Boolean> orderByAscending,
        @JsonProperty("separator") String separator,
        @JsonProperty("distinct") boolean distinct
    ) {
        this.index = index;
        this.orderByIndices = orderByIndices != null ? orderByIndices : new ArrayList<>();
        this.orderByAscending = orderByAscending != null ? orderByAscending : new ArrayList<>();
        this.separator = separator != null ? separator : ",";
        this.distinct = distinct;
    }

    @Override
    public Object first(@NonNull Object[] tuple) {
        Object value = tuple[index];
        if (value == null) {
            // NULL values are ignored in GROUP_CONCAT
            return null;
        }
        GroupConcatContext ctx = new GroupConcatContext(distinct);
        ctx.addRow(buildRow(tuple, value));
        return ctx;
    }

    @Override
    public Object add(@NonNull Object var, @NonNull Object[] tuple) {
        Object value = tuple[index];
        if (value == null) {
            // Ignore NULL values
            return var;
        }
        GroupConcatContext ctx = getOrCreate(var);
        ctx.addRow(buildRow(tuple, value));
        return ctx;
    }

    @Override
    public Object merge(@Nullable Object var1, @Nullable Object var2) {
        if (var1 == null) {
            return var2;
        }
        if (var2 == null) {
            return var1;
        }
        GroupConcatContext ctx1 = (GroupConcatContext) var1;
        GroupConcatContext ctx2 = (GroupConcatContext) var2;
        ctx1.merge(ctx2);
        return ctx1;
    }

    @Override
    public Object getValue(@Nullable Object var) {
        if (var == null) {
            return null;
        }
        GroupConcatContext ctx = (GroupConcatContext) var;
        List<Object[]> rows = ctx.getRows();
        if (rows.isEmpty()) {
            return null;
        }

        // ---- 1. Sort if ORDER BY columns are specified ----
        if (!orderByIndices.isEmpty()) {
            rows.sort(buildComparator());
        }

        // ---- 2. Join with separator ----
        long maxLen = resolveMaxLen();
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Object[] row : rows) {
            String strVal = row[0] == null ? "" : row[0].toString();
            if (!first) {
                sb.append(separator);
            }
            first = false;
            sb.append(strVal);
            // Truncate check
            if (sb.length() >= maxLen) {
                sb.setLength((int) maxLen);
                break;
            }
        }

        return sb.length() == 0 ? null : sb.toString();
    }

    @Override
    public AggregationOperator.AggregationType getAggregationType() {
        return AggregationOperator.AggregationType.GROUP_CONCAT;
    }

    @Override
    public int getIndex() {
        return index;
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /**
     * Build a row to store in the context.
     * Layout: [concatValue, orderKey0, orderKey1, ...]
     */
    private Object[] buildRow(Object[] tuple, Object value) {
        Object[] row = new Object[1 + orderByIndices.size()];
        row[0] = value;
        for (int i = 0; i < orderByIndices.size(); i++) {
            int idx = orderByIndices.get(i);
            row[1 + i] = (idx < tuple.length) ? tuple[idx] : null;
        }
        return row;
    }

    private GroupConcatContext getOrCreate(Object var) {
        if (var instanceof GroupConcatContext) {
            return (GroupConcatContext) var;
        }
        // Should not happen in normal flow, but guard against it
        GroupConcatContext ctx = new GroupConcatContext(distinct);
        return ctx;
    }

    /**
     * Build a multi-key comparator that handles ASC/DESC per column.
     * Nulls are treated as smaller than any non-null value (MySQL default).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Comparator<Object[]> buildComparator() {
        return (row1, row2) -> {
            for (int i = 0; i < orderByIndices.size(); i++) {
                Object v1 = row1[1 + i];
                Object v2 = row2[1 + i];
                int cmp;
                if (v1 == null && v2 == null) {
                    cmp = 0;
                } else if (v1 == null) {
                    cmp = -1; // NULL first
                } else if (v2 == null) {
                    cmp = 1;
                } else if (v1 instanceof Comparable) {
                    cmp = ((Comparable) v1).compareTo(v2);
                } else {
                    cmp = v1.toString().compareTo(v2.toString());
                }
                if (cmp != 0) {
                    // Flip sign for DESC
                    return orderByAscending.get(i) ? cmp : -cmp;
                }
            }
            return 0;
        };
    }

    /**
     * Resolve {@code group_concat_max_len} from the current session variables,
     * falling back to global variables, then to the MySQL default 1024.
     */
    private long resolveMaxLen() {
        final long defaultMaxLen = 1024L;
        try {
            // Try to read from session variable via ExecutionEnvironment
            ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
            if (env != null && env.sessionUtil != null) {
                // Attempt to find the active connection and read its session variable
                for (java.sql.Connection conn : env.sessionUtil.connectionMap.values()) {
                    try {
                        String val = conn.getClientInfo("group_concat_max_len");
                        if (val != null && !val.isEmpty()) {
                            return Math.max(1L, Long.parseLong(val));
                        }
                    } catch (Exception ignored) {
                        // try next connection
                    }
                }
            }
            // Fall back to global variable store
            InfoSchemaService infoSchemaService = InfoSchemaService.root();
            if (infoSchemaService != null) {
                Map<String, String> globalVars = infoSchemaService.getGlobalVariables();
                String val = globalVars.get("group_concat_max_len");
                if (val != null && !val.isEmpty()) {
                    return Math.max(1L, Long.parseLong(val));
                }
            }
        } catch (Exception ignored) {
        }
        return defaultMaxLen;
    }
}
