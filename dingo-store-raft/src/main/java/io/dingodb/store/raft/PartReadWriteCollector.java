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

package io.dingodb.store.raft;

import io.dingodb.common.CommonId;
import io.dingodb.server.protocol.metric.MetricLabel;
import io.dingodb.server.protocol.metric.MonitorMetric;
import io.prometheus.client.Collector;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class PartReadWriteCollector extends Collector {

    private static final PartReadWriteCollector INSTANCE = new PartReadWriteCollector();
    private Map<MonitorMetric, ArrayList<MetricFamilySamples.Sample>> samplesMap;

    public PartReadWriteCollector() {
        samplesMap = new ConcurrentHashMap<>();
    }

    public static PartReadWriteCollector instance() {
        return INSTANCE;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return this.samplesMap.keySet().stream().map(key -> {
            ArrayList<MetricFamilySamples.Sample> value = this.samplesMap.remove(key);
            return new MetricFamilySamples(key.value(), Type.GAUGE, "HELP", value);
        }).collect(Collectors.toList());
    }

    public void putSample(long cost, CommonId commonId, MonitorMetric metric) {
        ArrayList<MetricFamilySamples.Sample> samples = this.samplesMap.get(metric);
        if (samples == null) {
            samples = new ArrayList<>();
        }
        String label = buildIntervalLabel(cost);
        samples.add(new MetricFamilySamples.Sample(
            metric.value(),
            MetricLabel.PART_INTERVAL_LABELS,
            Arrays.asList(commonId.toString(), label),
            cost));
        this.samplesMap.put(metric, samples);
    }

    private static String buildIntervalLabel(long cost) {
        String label;
        if (cost <= 1) {
            label =  "0-1";
        } else if (cost <= 5) {
            label =  "1-5";
        } else if (cost <= 10) {
            label =  "5-10";
        } else if (cost <= 20 ) {
            label =  "10-20";
        } else if (cost <= 50) {
            label =  "20-50";
        } else if (cost <= 100) {
            label =  "50-100";
        } else if (cost <= 500) {
            label =  "100-500";
        } else if (cost <= 1000) {
            label =  "500-1000";
        } else {
            label =  "1000+";
        }
        return label;
    }
}
