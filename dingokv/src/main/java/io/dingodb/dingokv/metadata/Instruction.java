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

package io.dingodb.dingokv.metadata;

import com.alipay.sofa.jraft.util.Endpoint;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@ToString
public class Instruction implements Serializable {
    private static final long serialVersionUID = 2675841162817080976L;

    private Region region;
    private ChangePeer changePeer;
    private TransferLeader transferLeader;
    private RangeSplit rangeSplit;

    public static class ChangePeer implements Serializable {
        private static final long serialVersionUID = -6753587746283650702L;

        // TODO support add/update peer
    }

    @Getter
    @Setter
    @ToString
    public static class TransferLeader implements Serializable {
        private static final long serialVersionUID = 7483209239871846301L;

        private long moveToStoreId;
        private Endpoint moveToEndpoint;
    }

    @Getter
    @Setter
    @ToString
    public static class RangeSplit implements Serializable {
        private static final long serialVersionUID = -3451109819719367744L;

        private Long newRegionId;
    }
}
