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

package io.dingodb.store.row.metrics;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class KVMetricNames {

    // for state machine
    public static final String STATE_MACHINE_APPLY_QPS   = "dingo-st-apply-qps";
    public static final String STATE_MACHINE_BATCH_WRITE = "dingo-st-batch-write";

    // for rpc
    public static final String RPC_REQUEST_HANDLE_TIMER  = "dingo-rpc-request-timer";

    public static final String DB_TIMER                  = "dingo-db-timer";

    public static final String REGION_KEYS_READ          = "dingo-region-keys-read";
    public static final String REGION_KEYS_WRITTEN       = "dingo-region-keys-written";

    public static final String REGION_BYTES_READ         = "dingo-region-bytes-read";
    public static final String REGION_BYTES_WRITTEN      = "dingo-region-bytes-written";

    public static final String SEND_BATCHING             = "send_batching";
}
