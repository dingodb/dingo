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

package io.dingodb.store.row.client;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.option.CliOptions;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface DingoRowStoreCliService extends Lifecycle<CliOptions> {
    /**
     * Send a split instruction to the specified region.
     *
     * @param regionId    region id
     * @param newRegionId id of the new region after splitting
     * @param groupId     the raft group id
     * @param conf        current configuration
     * @return operation status
     */
    Status rangeSplit(final String regionId, final String newRegionId, final String groupId, final Configuration conf);
}
