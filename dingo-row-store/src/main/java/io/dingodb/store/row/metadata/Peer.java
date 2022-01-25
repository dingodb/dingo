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

import io.dingodb.raft.util.Copiable;
import io.dingodb.raft.util.Endpoint;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class Peer implements Copiable<Peer>, Serializable {
    private static final long serialVersionUID = 1L;

    private String id;
    private String storeId;
    private Endpoint endpoint;

    public Peer() {
    }

    @Override
    public Peer copy() {
        Endpoint endpoint = null;
        if (this.endpoint != null) {
            endpoint = this.endpoint.copy();
        }
        return new Peer(this.id, this.storeId, endpoint);
    }
}
