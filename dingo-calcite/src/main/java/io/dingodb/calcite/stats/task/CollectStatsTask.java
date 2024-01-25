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

package io.dingodb.calcite.stats.task;

import com.google.common.collect.Iterators;
import io.dingodb.calcite.stats.CountMinSketch;
import io.dingodb.calcite.stats.Histogram;
import io.dingodb.calcite.stats.StatsNormal;
import io.dingodb.calcite.stats.TableStats;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.exec.Services;
import io.dingodb.exec.table.Part;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

/**
 * collect region statistic. If the table has multiple partitions,
 * create multiple concurrent tasks,Then merge region statistics
 */
@Slf4j
public class CollectStatsTask implements Callable<TableStats> {
    Iterator<Object[]> tupleIterator;
    List<Histogram> columnHistogramList;
    List<CountMinSketch> minSketchList;
    Map<String, StatsNormal> statsNormalMap;

    /**
     * collect stats task by one region.
     * statistic type: histogram(only int), countMinSketch, statsNormal
     * @param region region distribution
     * @param tableId tableId
     * @param td tableDefinition
     * @param columnHistograms columnHistogram :  Unified template (All region histograms must have the same parameters)
     * @param minSketches minSketch : Unified template
     * @param statsNormals statsNormal : distinct val,null count
     */
    public CollectStatsTask(RangeDistribution region,
                            CommonId tableId,
                            Table td,
                            List<Histogram> columnHistograms,
                            List<CountMinSketch> minSketches,
                            List<StatsNormal> statsNormals,
                            long scanTs,
                            long timeout) {
        boolean isTxn = td.getEngine().contains("TXN");
        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, region.id());
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(tableId, td.tupleType(), td.keyMapping());
        if (!isTxn) {
            Part part = new PartInKvStore(
                kvStore,
                codec
            );
            tupleIterator = part.scan(region.getStartKey(), region.getEndKey(),
                region.isWithStart(), true);
        } else {
            Iterator<KeyValue> iterator = kvStore.txnScan(
                scanTs,
                new StoreInstance.Range(region.getStartKey(), region.getEndKey(), region.isWithStart(), true),
                timeout
            );
            tupleIterator = Iterators.transform(iterator,
                wrap(codec::decode)::apply
            );
        }
        this.minSketchList = minSketches.stream().map(CountMinSketch::copy)
            .collect(Collectors.toList());
        columnHistogramList = columnHistograms.stream().map(Histogram::copy)
            .collect(Collectors.toList());
        statsNormalMap = statsNormals.stream()
            .collect(Collectors.toMap(StatsNormal::getColumnName, StatsNormal::copy));
    }

    @Override
    public TableStats call() {
        log.info("collect iterator start");
        while (tupleIterator.hasNext()) {
            Object[] tuples = tupleIterator.next();
            if (columnHistogramList.size() > 0) {
                columnHistogramList.forEach(e -> {
                    Object val = tuples[e.getIndex()];
                    e.addValue(val);
                    statsNormalMap.get(e.getColumnName()).addVal(val);
                });
            }
            if (minSketchList.size() > 0) {
                minSketchList.forEach(e -> {
                    String val = (String) tuples[e.getIndex()];
                    e.setString(val);
                    statsNormalMap.get(e.getColumnName()).addVal(val);
                });
            }
        }
        log.info("collect iterator end...");
        return new TableStats(minSketchList, columnHistogramList,
            new ArrayList<>(statsNormalMap.values()));
    }

}
