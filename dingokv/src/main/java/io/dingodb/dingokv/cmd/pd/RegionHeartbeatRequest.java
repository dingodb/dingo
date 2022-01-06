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

package io.dingodb.dingokv.cmd.pd;

import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.dingokv.metadata.Region;
import io.dingodb.dingokv.metadata.RegionStats;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@ToString
public class RegionHeartbeatRequest extends BaseRequest {
    private static final long serialVersionUID = -5149082334939576598L;

    private long leastKeysOnSplit;
    private Region region;
    private RegionStats regionStats;
    private Endpoint selfEndpoint;

    @Override
    public byte magic() {
        return REGION_HEARTBEAT;
    }
}
