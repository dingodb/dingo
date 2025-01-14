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

package io.dingodb.store.api.transaction.data;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@Builder
@EqualsAndHashCode
@ToString
public class DocumentSearchParameter {
    // The number of top results to return.
    private int topN;
    // The query_string for search
    private String queryString;
    // use id filter
    private boolean useIdFilter;
    // if use_id_filter = true, use this field
    private List<Long> documentIds;
    // use column filter
    // if query_string is simple string, use column_names to select columns
    // if query_string is full functional expr, like "col1: value1 AND col2: value2", column_names is ignored
    private List<String> columnNames;
    // for output, if only id is needed, set without_scalar_data = true
    // else set without_scalar_data = false, and set selected_keys to select scalar data
    // Default false, if true, response without scalar data.
    private boolean withoutScalarData;
    // If without_scalar_data is false, selected_keys is used to select scalar data,
    // if this parameter is null, all scalar data will be returned.
    private List<String> selectedKeys;
    // if query_unlimited = true, top_n is ignored
    private boolean queryUnlimited;
}
