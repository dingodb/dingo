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

package io.dingodb.common.util;

import io.dingodb.common.table.HybridSearchTable;

public class HybridSearchSqlUtils {

    public final static String TEMPLATE_SQL = "select * from (\n" +
        "       select coalesce(similarity.id, bm25.id) as id, \n" +
        "              cast((coalesce(similarity.score, 0.0) * %s + coalesce(bm25.score, 0.0) * %s) as float) as rank_hybrid\n" +
        "           from (\n" +
        "                select /*+ " + HybridSearchTable.HINT_NAME + " */ %s id, %s score\n" +
        "                    from %s order by %s limit %s\n" +
        "                ) similarity\n" +
        "           full outer join\n" +
        "                (\n" +
        "                select %s id, %s score\n" +
        "                    from %s order by %s limit %s\n" +
        "                ) bm25\n" +
        "        on similarity.id = bm25.id\n" +
        ") score_hybrid order by score_hybrid.rank_hybrid desc";

    public final static String SIMILARITY_TABLE_NAME = "similarity";
    public final static String BM25_TABLE_NAME = "bm25";

    public static String hybridSearchSqlReplace(
        float vectorWeight,
        float documentWeight,
        String vectorId,
        String vectorDistance,
        String vectorSelect,
        int vectorLimit,
        String documentId,
        String documentRankBm25,
        String documentSelect,
        int documentLimit
    ) {
        return String.format(
            TEMPLATE_SQL,
            vectorWeight,
            documentWeight,
            vectorId,
            vectorDistance,
            vectorSelect,
            vectorDistance,
            vectorLimit,
            documentId,
            documentRankBm25,
            documentSelect,
            documentRankBm25,
            documentLimit
        );
    }

}
