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

package io.dingodb.server.coordinator.resource.impl;

import com.alipay.remoting.util.ConcurrentHashSet;
import com.alipay.sofa.jraft.util.Endpoint;
import io.dingodb.meta.Location;
import io.dingodb.server.coordinator.GeneralId;
import io.dingodb.server.coordinator.resource.NodeResourceView;
import io.dingodb.server.coordinator.score.Score;
import io.dingodb.store.row.metadata.StoreStats;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.Delegate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@ToString
@NoArgsConstructor
public class ExecutorView implements NodeResourceView {

    private GeneralId id;
    private Location location;

    private Set<GeneralId> apps = new ConcurrentHashSet<>();

    private Map<String, String> labels = new HashMap<>();

    private DiskView diskView;
    private StoreStats stats;

    public ExecutorView(GeneralId id, Endpoint endpoint) {
        this.id = id;
        this.location = new Location(endpoint.getIp(), endpoint.getPort(), "");
    }

    public StoreStats stats() {
        return stats;
    }

    public ExecutorView stats(StoreStats stats) {
        if (stats == null) {
            return this;
        }
        this.stats = stats;
        diskView = new DiskView(GeneralId.resourceViewOf(0, "disk@store" + id.seqNo()), stats.getCapacity());
        diskView.refreshUsable(stats.getAvailable());
        return this;
    }

    public void addApp(GeneralId id) {
        apps.add(id);
    }

    public void addAllApp(Collection<GeneralId> ids) {
        apps.addAll(ids);
    }

    @Override
    public GeneralId resourceId() {
        return id;
    }

    @Override
    public Map<String, String> labels() {
        return labels;
    }

    @Override
    public Score score() {
        return diskView.score();
    }

    @Override
    public Set<GeneralId> apps() {
        return apps;
    }

    @Override
    public DiskView disk() {
        return diskView;
    }

    @Override
    @Delegate
    public Location location() {
        return location;
    }
}
