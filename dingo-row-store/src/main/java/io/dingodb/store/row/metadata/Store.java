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

package io.dingodb.store.row.metadata;

import com.alipay.sofa.jraft.util.Copiable;
import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.store.row.util.Lists;
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
public class Store implements Copiable<Store>, Serializable {
    private static final long serialVersionUID = 1L;

    private String id;
    private Endpoint endpoint;
    private StoreState state;
    private List<Region> regions;
    private List<StoreLabel> labels;

    public Store() {
    }

    public boolean isEmpty() {
        return this.endpoint == null || this.regions == null || this.regions.isEmpty();
    }

    @Override
    public Store copy() {
        Endpoint endpoint = null;
        if (this.endpoint != null) {
            endpoint = this.endpoint.copy();
        }
        List<Region> regions = null;
        if (this.regions != null) {
            regions = Lists.newArrayListWithCapacity(this.regions.size());
            for (Region region : this.regions) {
                regions.add(region.copy());
            }
        }
        List<StoreLabel> labels = null;
        if (this.labels != null) {
            labels = Lists.newArrayListWithCapacity(this.labels.size());
            for (StoreLabel label : this.labels) {
                labels.add(label.copy());
            }
        }
        return new Store(this.id, endpoint, this.state, regions, labels);
    }
}
