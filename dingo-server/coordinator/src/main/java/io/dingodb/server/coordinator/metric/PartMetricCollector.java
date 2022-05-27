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

package io.dingodb.server.coordinator.metric;

import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.ExecutorAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.ReplicaAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePartStatsAdaptor;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.TablePartStats;
import io.dingodb.server.protocol.metric.MetricLabel;
import io.dingodb.server.protocol.metric.MonitorMetric;
import io.prometheus.client.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PartMetricCollector extends Collector {

    @Override
    public List<MetricFamilySamples> collect() {
        Map<MonitorMetric, ArrayList<MetricFamilySamples.Sample>> samplesMap = new ConcurrentHashMap<>();
        putSample(samplesMap);

        return samplesMap.entrySet().stream()
            .map(entry -> new MetricFamilySamples(entry.getKey().value(), Type.GAUGE, "HELP", entry.getValue()))
            .collect(Collectors.toList());
    }

    private static void putSample(Map<MonitorMetric, ArrayList<MetricFamilySamples.Sample>> samplesMap) {
        ArrayList<MetricFamilySamples.Sample> countSample = new ArrayList<>();
        ExecutorAdaptor executorAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Executor.class);
        ReplicaAdaptor replicaAdaptor = MetaAdaptorRegistry.getMetaAdaptor(Replica.class);
        executorAdaptor.getAll().forEach(executor -> {
            Integer partCount = Optional.of(replicaAdaptor.getByExecutor(executor.getId()).size()).orElse(0);
            countSample.add(new MetricFamilySamples.Sample(
                MonitorMetric.PART_COUNT.value(),
                Collections.singletonList(MetricLabel.EXECUTOR),
                Collections.singletonList(executor.getId().toString()),
                partCount
            ));
        });
        samplesMap.put(MonitorMetric.PART_COUNT, countSample);

        ArrayList<MetricFamilySamples.Sample> sizeSample = new ArrayList<>();
        TablePartStatsAdaptor tablePartStatsAdaptor = MetaAdaptorRegistry.getStatsMetaAdaptor(TablePartStats.class);
        tablePartStatsAdaptor.getAllStats().forEach(stats -> {
            Long partSize = stats.getApproximateStats().stream()
                .map(TablePartStats.ApproximateStats::getSize)
                .reduce(0L, Long::sum);
            sizeSample.add(new MetricFamilySamples.Sample(
                MonitorMetric.PART_SIZE.value(),
                Collections.singletonList(MetricLabel.PART),
                Collections.singletonList(stats.getTablePart().toString()),
                partSize
            ));
        });
        samplesMap.put(MonitorMetric.PART_SIZE, sizeSample);
    }
}
