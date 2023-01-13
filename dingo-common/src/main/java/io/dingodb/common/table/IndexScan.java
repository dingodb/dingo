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

package io.dingodb.common.table;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@RequiredArgsConstructor
public class IndexScan {
    public final String tableCat;
    public final String tableSchema;
    public final String tableName;
    public final boolean unique;
    public final String indexQualifier;
    public final String indexName;
    public final short type;
    public final short ordinalPosition;
    public final String columnName;
    public final String ascOrDesc;
    public final String cardinality;
    public final int pages;
    public final String filterCondition;
}
