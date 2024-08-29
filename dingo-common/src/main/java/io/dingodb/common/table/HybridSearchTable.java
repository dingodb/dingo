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

import java.util.ArrayList;
import java.util.List;


public final class HybridSearchTable {
    public final static String TABLE_NAME = "hybrid_search";

    public final static String TYPE_ID = "BIGINT";

    public final static String TYPE_RANK_HYBRID = "FLOAT";

    public final static int INDEX_ID = 0;

    public final static int INDEX_RANK_HYBRID = 1;

    public final static String HINT_NAME = "hybrid_normalization_vector";

    @Getter
    public final static List<String> columns =new ArrayList<>(2);

    static {
        columns.add("id");
        columns.add("rank_hybrid");
    }

}
