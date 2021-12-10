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

import com.alipay.sofa.jraft.util.Copiable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Objects;

@Setter
@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class RegionEpoch implements Copiable<RegionEpoch>, Comparable<RegionEpoch>, Serializable {

    private static final long serialVersionUID = 1L;

    private long              confVer;
    private long              version;

    public RegionEpoch() {
    }

    @Override
    public RegionEpoch copy() {
        return new RegionEpoch(this.confVer, this.version);
    }

    @Override
    public int compareTo(RegionEpoch regionEpoch) {
        if (this.version == regionEpoch.version) {
            return (int) (this.confVer - regionEpoch.confVer);
        }
        return (int) (this.version - regionEpoch.version);
    }
}
