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
import io.dingodb.dingokv.util.Lists;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class Cluster implements Copiable<Cluster>, Serializable {
    private static final long serialVersionUID = 3291666486933960310L;

    private long clusterId;
    private List<Store> stores;

    @Override
    public Cluster copy() {
        List<Store> stores = null;
        if (this.stores != null) {
            stores = Lists.newArrayListWithCapacity(this.stores.size());
            for (Store store : this.stores) {
                stores.add(store.copy());
            }
        }
        return new Cluster(this.clusterId, stores);
    }
}

