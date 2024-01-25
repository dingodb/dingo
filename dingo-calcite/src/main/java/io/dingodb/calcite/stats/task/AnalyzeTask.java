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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.calcite.stats.AnalyzeInfo;
import io.dingodb.calcite.stats.CountMinSketch;
import io.dingodb.calcite.stats.Histogram;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.stats.StatsNormal;
import io.dingodb.calcite.stats.StatsOperator;
import io.dingodb.calcite.stats.StatsTaskState;
import io.dingodb.calcite.stats.TableStats;
import io.dingodb.codec.CodecService;
import io.dingodb.common.AggregationOperator;
import io.dingodb.common.CommonId;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.exec.Services;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.aggregate.MaxAgg;
import io.dingodb.exec.aggregate.MinAgg;
import io.dingodb.exec.table.Part;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.exec.utils.SchemaWrapperUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.tso.TsoService;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Builder
@Slf4j
@ToString
public class AnalyzeTask extends StatsOperator implements Runnable {
    private String schemaName;
    private String tableName;
    private List<String> columnList;

    @Builder.Default
    private int cmSketchHeight = 5;
    @Builder.Default
    private int cmSketchWidth = 10000;
    @Builder.Default
    private Integer bucketCount = 254;
    private long samples;
    private float sampleRate;

    @Builder.Default
    private long timeout = 50000;


    @Override
    public void run() {
        long rowCount = 0;
        String failReason = "";
        try {
            // get table info
            MetaService metaService = MetaService.root();
            metaService = metaService.getSubMetaService(schemaName);
            Table td = metaService.getTable(tableName);
            CommonId tableId = td.getTableId();

            startAnalyzeTask(tableId);
            List<RangeDistribution> rangeDistributions = new ArrayList<>(metaService
                .getRangeDistribution(tableId).values());

            List<Histogram> histogramList = new ArrayList<>();
            List<CountMinSketch> cmSketchList = new ArrayList<>();
            List<StatsNormal> statsNormals = new ArrayList<>();

            // varchar -> count-min-sketch  int,float,double,date,time,timestamp -> histogram
            // ndv, nullCount -> normal
            typeMetricAdaptor(td, histogramList, cmSketchList, statsNormals, cmSketchWidth, cmSketchHeight);
            // par scan get min, max
            // histogram equ-width need max, min
            buildHistogram(histogramList, rangeDistributions, tableId, td);

            log.info("collect stats start");
            List<TableStats> statsList = null;
            try {
                List<CompletableFuture<TableStats>> futureList = getCompletableFutures(td, tableId, rangeDistributions,
                    cmSketchList, statsNormals, histogramList);
                statsList = new ArrayList<>();
                for (CompletableFuture<TableStats> completableFuture : futureList) {
                    try {
                        statsList.add(completableFuture.get());
                    } catch (InterruptedException | ExecutionException e) {
                        failReason = e.getMessage();
                        log.error(e.getMessage(), e);
                        break;
                    }
                }
            } catch (Exception e) {
                failReason = e.getMessage();
                log.error(e.getMessage(), e);
            }
            // merge regions stats
            if (statsList == null) {
                return;
            }
            TableStats.mergeStats(statsList);
            TableStats tableStats = statsList.get(0);

            // save stats to store
            addHistogram(tableStats.getHistogramList());
            addCountMinSketch(tableStats.getCountMinSketchList());
            addStatsNormal(tableStats.getStatsNormalList());
            // update analyze job status
            cache(tableStats);
            rowCount = tableStats.getRowCount();
            log.info("stats collect done");
        } catch (Exception e) {
            failReason = e.getMessage();
            log.error(e.getMessage(), e);
        }
        endAnalyzeTask(failReason, rowCount);
    }

    private List<CompletableFuture<TableStats>> getCompletableFutures(
        Table td,
        CommonId tableId,
        List<RangeDistribution> rangeDistributions,
        List<CountMinSketch> cmSketchList,
        List<StatsNormal> statsNormals,
        List<Histogram> columnHistograms
    ) {
        long scanTs = TsoService.getDefault().tso();
        List<CompletableFuture<TableStats>> futureList = rangeDistributions.stream().map(_i -> {
            Callable<TableStats> collectStatsTask = new CollectStatsTask(
                _i, tableId, td, columnHistograms, cmSketchList, statsNormals, scanTs, timeout
            );
            return Executors.submit("collect-task", collectStatsTask);
        }).collect(Collectors.toList());

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futureList.toArray(new CompletableFuture[0]));

        try {
            allFutures.join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return futureList;
    }

    private void typeMetricAdaptor(Table td,
                                   List<Histogram> histogramCdList,
                                   List<CountMinSketch> cmSketchCdList,
                                   List<StatsNormal> statsNormals,
                                   int cmSketchWidth,
                                   int cmSketchHeight) {
        AtomicInteger index = new AtomicInteger();
        td.getColumns().forEach(columnDefinition -> {
            index.incrementAndGet();
            if (columnList != null && columnList.size() > 0 && !columnList.contains(columnDefinition.getName())) {
                return;
            }
            boolean allowStats = false;
            if ((columnDefinition.getType() instanceof IntegerType)
                || (columnDefinition.getType() instanceof DoubleType)
                || (columnDefinition.getType() instanceof FloatType)
                || (columnDefinition.getType() instanceof LongType)
                || (columnDefinition.getType() instanceof DecimalType)
                || (columnDefinition.getType() instanceof DateType)
                || (columnDefinition.getType() instanceof TimeType)
                || (columnDefinition.getType() instanceof TimestampType)
            ) {
                allowStats = true;
                histogramCdList.add(new Histogram(schemaName, tableName,
                    columnDefinition.getName(), columnDefinition.getType(), index.get() - 1));
            } else if (columnDefinition.getType() instanceof StringType) {
                allowStats = true;
                cmSketchCdList.add(new CountMinSketch(schemaName, tableName, columnDefinition.getName(),
                    index.get() - 1,
                    cmSketchWidth, cmSketchHeight));
            }
            if (!allowStats) {
                return;
            }
            statsNormals.add(new StatsNormal(
                columnDefinition.getName(),
                columnDefinition.getType()));
        });
    }

    private void addHistogram(List<Histogram> histogramList) {
        List<Object[]> paramList = histogramList.stream().map(histogram -> {
            String histogramDetail = histogram.serialize();
            return new Object[] {histogram.getSchemaName(), histogram.getTableName(), histogram.getColumnName(),
                histogramDetail};
        }).collect(Collectors.toList());
        upsert(bucketsStore, bucketsCodec, paramList);
    }

    private void addCountMinSketch(List<CountMinSketch> countMinSketches) {
        List<Object[]> paramList = countMinSketches.stream().map(countMinSketch -> {
            String cmSketch = countMinSketch.serialize();
            return new Object[] {countMinSketch.getSchemaName(), countMinSketch.getTableName(),
                countMinSketch.getColumnName(), cmSketch, countMinSketch.getNullCount(),
                countMinSketch.getTotalCount(), countMinSketch.getIndex()
            };
        }).collect(Collectors.toList());
        upsert(cmSketchStore, cmSketchCodec, paramList);
    }

    private void addStatsNormal(List<StatsNormal> statsNormals) {
        List<Object[]> paramList = statsNormals.stream().map(statsNormal ->
            new Object[] {schemaName, tableName, statsNormal.getColumnName(), statsNormal.getNdv(),
                statsNormal.getNumNull(), statsNormal.getAvgColSize(), statsNormal.getTotalCount()}
        ).collect(Collectors.toList());
        upsert(statsStore, statsCodec, paramList);
    }

    private static void cache(TableStats tableStats) {
        tableStats.initRowCount();
        StatsCache.statsMap.put(tableStats.getIdentifier(), tableStats);
    }

    private void buildHistogram(List<Histogram> histogramList,
                                List<RangeDistribution> rangeDistributions,
                                CommonId tableId,
                                Table td) {
        if (histogramList.size() > 0) {
            List<Iterator<Object[]>> iteratorList = rangeDistributions.stream().map(region -> {
                TupleMapping outputKeyMapping = TupleMapping.of(
                    IntStream.range(0, 0).boxed().collect(Collectors.toList())
                );
                DingoType outputSchema = DingoTypeFactory.tuple(
                    histogramList.stream().flatMap(intHistogram ->
                        Arrays.stream(new DingoType[]{intHistogram.getDingoType(),
                            intHistogram.getDingoType()})).toArray(DingoType[]::new));
                AtomicInteger index = new AtomicInteger(0);
                List<Agg> aggList = histogramList.stream().flatMap(intHistogram -> {
                    MaxAgg maxAgg = new MaxAgg(index.get(), intHistogram.getDingoType());
                    MinAgg minAgg = new MinAgg(index.get(), intHistogram.getDingoType());
                    index.incrementAndGet();
                    return Arrays.stream(new Agg[]{maxAgg, minAgg});
                }).collect(Collectors.toList());
                Coprocessor.CoprocessorBuilder builder = Coprocessor.builder();
                builder.selection(histogramList.stream().map(Histogram::getIndex).collect(Collectors.toList()));
                builder.aggregations(aggList.stream().map(
                    agg -> {
                        AggregationOperator.AggregationOperatorBuilder operatorBuilder = AggregationOperator.builder();
                        operatorBuilder.operation(agg.getAggregationType());
                        operatorBuilder.indexOfColumn(agg.getIndex());
                        return operatorBuilder.build();
                    }
                ).collect(Collectors.toList()));
                builder.originalSchema(SchemaWrapperUtils.buildSchemaWrapper(
                    td.tupleType(), td.keyMapping(), tableId.seq));
                builder.resultSchema(SchemaWrapperUtils.buildSchemaWrapper(
                    outputSchema, outputKeyMapping, tableId.seq
                ));
                Coprocessor coprocessor = builder.build();

                StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, region.getId());
                Part part = new PartInKvStore(kvStore,
                    CodecService.getDefault().createKeyValueCodec(tableId, outputSchema, outputKeyMapping));
                return part.scan(region.getStartKey(), region.getEndKey(),
                    region.isWithStart(), true, coprocessor);
            }).collect(Collectors.toList());
            for (Iterator<Object[]> iterator : iteratorList) {
                if (iterator.hasNext()) {
                    Object[] tuples = iterator.next();
                    for (int i = 0; i < histogramList.size(); i ++) {
                        histogramList.get(i).setRegionMax(tuples[2 * i]);
                        histogramList.get(i).setRegionMin(tuples[2 * i + 1]);
                    }
                }
            }
            histogramList.forEach(histogram -> histogram.init(bucketCount));
        }
    }

    private void startAnalyzeTask(CommonId tableId) {
        Object[] values = get(analyzeTaskStore, analyzeTaskCodec, getAnalyzeTaskKeys(schemaName, tableName));
        if (values == null) {
            Long commitCount = 0L;
            try {
                commitCount = MetaService.root().getTableCommitCount().getOrDefault(tableId, 0L);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            long totalCount = 0;
            if (commitCount > totalCount) {
                commitCount = totalCount;
            }
            values = generateAnalyzeTask(schemaName, tableName, totalCount, commitCount);
        }
        Timestamp current = new Timestamp(System.currentTimeMillis());
        values[2] = getAnalyzeParam();
        values[4] = current;
        values[6] = StatsTaskState.RUNNING.getState();
        values[10] = current;
        try {
            upsert(analyzeTaskStore, analyzeTaskCodec, Collections.singletonList(values));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void endAnalyzeTask(String failReason, long rowCount) {
        Object[] values = get(analyzeTaskStore, analyzeTaskCodec, getAnalyzeTaskKeys(schemaName, tableName));
        if (values == null) {
            log.error("analyze task is null");
            return;
        }
        Timestamp current = new Timestamp(System.currentTimeMillis());
        if (StringUtils.isBlank(failReason)) {
            values[6] = StatsTaskState.SUCCESS.getState();
        } else {
            values[6] = StatsTaskState.FAIL.getState();
            values[7] = failReason;
        }
        values[5] = current;
        values[10] = current;
        values[3] = rowCount;
        upsert(analyzeTaskStore, analyzeTaskCodec, Collections.singletonList(values));
    }

    private String getAnalyzeParam() {
        AnalyzeInfo analyzeInfo = new AnalyzeInfo(cmSketchHeight, cmSketchWidth, bucketCount, columnList);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(analyzeInfo);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
            return "";
        }
    }
}
