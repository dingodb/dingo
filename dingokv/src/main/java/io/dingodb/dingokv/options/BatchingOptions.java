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

package io.dingodb.dingokv.options;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@ToString
public class BatchingOptions {
    // If batching is allowed, the client will submit the requests in batch mode,
    // which will improve the throughput without any negative impact on the delay.
    private boolean allowBatching = true;
    // Maximum number of requests that can be applied in a batch.
    private int     batchSize     = 100;
    // Internal disruptor buffers size for get/put request etc.
    private int     bufSize       = 8192;
    // Maximum bytes size to cached for put-request (keys.size + value.size).
    private int     maxWriteBytes = 32768;
    // Maximum bytes size to cached for get-request (keys.size).
    private int     maxReadBytes  = 1024;
}
