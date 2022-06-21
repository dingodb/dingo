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

package io.dingodb.server.coordinator.fake;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.Part;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ReportApi;
import io.dingodb.server.api.TableStoreApi;
import io.dingodb.server.protocol.meta.TablePartStats;

import java.util.Collections;
import java.util.ServiceLoader;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.STATS_IDENTIFIER;

public class FakeTableStoreApi implements TableStoreApi {

    private ReportApi reportApi;

    public FakeTableStoreApi() {

        ApiRegistry apiRegistry = ServiceLoader.load(NetServiceProvider.class).iterator().next().get().apiRegistry();
        reportApi = apiRegistry.proxy(
            ReportApi.class,
            () -> new Location(DingoConfiguration.host(), DingoConfiguration.port())
        );
        apiRegistry.register(TableStoreApi.class, this);
    }

    @Override
    public void addTablePartReplica(CommonId table, CommonId part, Location replica) {

    }

    @Override
    public void removeTablePartReplica(CommonId table, CommonId part, Location replica) {

    }

    @Override
    public void transferLeader(CommonId table, CommonId part, Location leader) {

    }

    @Override
    public void newTable(CommonId id) {

    }

    @Override
    public void deleteTable(CommonId id) {

    }

    @Override
    public void assignTablePart(Part part) {
        TablePartStats stats = TablePartStats.builder()
            .id(new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, part.getId().domainContent(), part.getId().seqContent()))
            .leader(DingoConfiguration.instance().getServerId())
            .tablePart(part.getId())
            .table(part.getInstanceId())
            .approximateStats(Collections.emptyList())
            .build();
        reportApi.report(stats);
    }

    @Override
    public void reassignTablePart(Part part) {

    }

    @Override
    public void removeTablePart(Part part) {

    }

    @Override
    public void deleteTablePart(Part part) {

    }
}
