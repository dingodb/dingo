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

package io.dingodb.calcite.utils;

import java.util.Arrays;
import java.util.List;

public final class IndexParameterUtils {

    public final static List<String> vectorKeys = Arrays.asList(
        "indexType", "type", "metricType", "dimension", "ncentroids", "nsubvector", "bucketInitSize",
        "bucket_init_size", "bucketMaxSize", "bucket_max_size", "nbitsPerIdx", "nbits_per_idx", "efConstruction",
        "maxElements", "max_elements", "nlinks", "valueType", "value_type", "maxDegree", "max_degree", "searchListSize",
        "search_list_size", "qd", "codebookPrefix", "codebook_prefix", "pqDiskBytes", "pq_disk_bytes",
        "appendReorderData", "append_reorder_data", "buildPqBytes", "build_pq_bytes", "useOpq", "use_opq");

    public final static List<String> documentKeys = Arrays.asList("indexType", "text_fields");
}
