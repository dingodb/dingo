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

package io.dingodb.coordinator.app.impl;

import io.dingodb.coordinator.GeneralId;
import io.dingodb.coordinator.app.AppView;
import io.dingodb.coordinator.score.impl.SimplePercentScore;
import io.dingodb.dingokv.metadata.RegionStats;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Set;

@ToString
@NoArgsConstructor
public class RegionView implements AppView<RegionView, RegionApp> {

    private GeneralId id;

    private GeneralId appId;

    private GeneralId leader;

    private Set<GeneralId> nodes;
    private Set<GeneralId> followers;
    private Set<GeneralId> learners;

    private GeneralId diskViewId;

    private long confVer;

    @Setter
    @Getter
    private RegionStats regionStats;

    public RegionView(GeneralId id, GeneralId appId, RegionStats regionStats) {
        this.id = id;
        this.regionStats = regionStats;
        this.confVer = 0;
        this.appId = appId;
    }

    public RegionView(GeneralId id, GeneralId appId, RegionStats regionStats, long confVer) {
        this.id = id;
        this.regionStats = regionStats;
        this.confVer = confVer;
        this.appId = appId;
    }

    public RegionView(
        GeneralId id,
        GeneralId leader,
        GeneralId appId,
        Set<GeneralId> nodes,
        Set<GeneralId> followers,
        Set<GeneralId> learners,
        GeneralId diskViewId,
        long confVer,
        RegionStats regionStats
    ) {
        this.id = id;
        this.leader = leader;
        this.nodes = nodes;
        this.appId = appId;
        this.followers = followers;
        this.learners = learners;
        this.diskViewId = diskViewId;
        this.confVer = confVer;
        this.regionStats = regionStats;
    }

    @Override
    public GeneralId viewId() {
        return id;
    }

    @Override
    public GeneralId app() {
        return appId;
    }

    public RegionView app(GeneralId appId) {
        this.appId = appId;
        return this;
    }

    @Override
    public GeneralId leader() {
        return leader;
    }

    public RegionView leader(GeneralId leader) {
        this.leader = leader;
        return this;
    }

    @Override
    public Set<GeneralId> nodeResources() {
        return nodes;
    }

    public RegionView nodes(Set<GeneralId> nodes) {
        this.nodes = nodes;
        return this;
    }

    public Set<GeneralId> followers() {
        return followers;
    }

    public RegionView followers(Set<GeneralId> followers) {
        this.followers = followers;
        return this;
    }

    @Override
    public SimplePercentScore score() {
        return null;
    }

    public long confVer() {
        return confVer;
    }

    public RegionView confVer(long confVer) {
        this.confVer = confVer;
        return this;
    }

    public void increaseConfVer() {
        confVer++;
    }
}
