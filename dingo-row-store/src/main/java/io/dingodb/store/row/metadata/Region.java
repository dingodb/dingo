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

import io.dingodb.raft.util.BytesUtil;
import io.dingodb.raft.util.Copiable;
import io.dingodb.store.row.util.Lists;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode
public class Region implements Copiable<Region>, Serializable {
    private static final long serialVersionUID = 1L;

    public static final long  MIN_ID_WITH_MANUAL_CONF = -1L;
    public static final long  MAX_ID_WITH_MANUAL_CONF = 1000000L;

    private String id;
    private byte[] startKey;
    private byte[] endKey;
    private RegionEpoch regionEpoch;
    private List<Peer> peers;

    public Region() {
    }

    @Override
    public Region copy() {
        RegionEpoch regionEpoch = null;
        if (this.regionEpoch != null) {
            regionEpoch = this.regionEpoch.copy();
        }
        List<Peer> peers = null;
        if (this.peers != null) {
            peers = Lists.newArrayListWithCapacity(this.peers.size());
            for (Peer peer : this.peers) {
                peers.add(peer.copy());
            }
        }
        return new Region(this.id, this.startKey, this.endKey, regionEpoch, peers);
    }

    @Override
    public String toString() {
        return "Region{" + "id=" + id + ", startKey=" + BytesUtil.toHex(startKey) + ", endKey="
            + BytesUtil.toHex(endKey) + ", regionEpoch=" + regionEpoch + ", peers=" + peers + '}';
    }

    public int getPeerCount() {
        return peers.size();
    }
}
