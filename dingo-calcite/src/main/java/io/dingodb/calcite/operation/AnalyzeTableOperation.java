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

package io.dingodb.calcite.operation;

import io.dingodb.calcite.grammar.ddl.SqlAnalyze;
import io.dingodb.calcite.stats.CountMinSketch;
import io.dingodb.calcite.stats.Histogram;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.stats.StatsNormal;
import io.dingodb.calcite.stats.StatsOperator;
import io.dingodb.calcite.stats.TableStats;
import io.dingodb.calcite.stats.task.CollectStatsTask;
import io.dingodb.codec.CodecService;
import io.dingodb.common.AggregationOperator;
import io.dingodb.common.CommonId;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
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
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class AnalyzeTableOperation extends StatsOperator implements DdlOperation {

    Connection connection;
    String tableName;
    String schemaName;

    List<String> columnList;

    private int cmSketchHeight;
    private int cmSketchWidth;
    private int bucketCount;
    private long samples;
    private float sampleRate;

    Long totalCount;

    public AnalyzeTableOperation(Connection connection, SqlAnalyze sqlAnalyze) {
        this.schemaName = sqlAnalyze.getSchemaName();
        this.tableName = sqlAnalyze.getTableName();
        MetaService metaService = MetaService.root().getSubMetaService(schemaName);
        this.totalCount = metaService.getTableStatistic(tableName).getRowCount().longValue();
        this.connection = connection;
        this.columnList = sqlAnalyze.getColumns();
        this.cmSketchHeight = sqlAnalyze.getCmSketchHeight();
        this.cmSketchWidth = sqlAnalyze.getCmSketchWidth();
        if (cmSketchHeight == 0 && cmSketchWidth == 0) {
            this.cmSketchWidth = 10000;
            this.cmSketchHeight = 5;
        }
        this.bucketCount = sqlAnalyze.getBuckets();
        if (bucketCount == 0) {
            bucketCount = 254;
        }
        this.samples = sqlAnalyze.getSamples();
        this.sampleRate = sqlAnalyze.getSampleRate();
    }

    @Override
    public void execute() {
        try {
            // get table info
            MetaService metaService = MetaService.root();
            metaService = metaService.getSubMetaService(schemaName);
            TableDefinition td = metaService.getTableDefinition(tableName);
            CommonId tableId = metaService.getTableId(tableName);
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
            String failReason = "";
            List<TableStats> statsList = null;
            try {
                List<CompletableFuture<TableStats>> futureList = getCompletableFutures(td, tableId, rangeDistributions,
                    cmSketchList, statsNormals, histogramList);
                statsList = new ArrayList<>();
                for (CompletableFuture<TableStats> completableFuture : futureList) {
                    try {
                        statsList.add(completableFuture.get());
                    } catch (InterruptedException | ExecutionException e) {
                        log.error(e.getMessage(), e);
                        break;
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            // merge regions stats
            if (statsList == null) {
                return;
            }
            TableStats.mergeStats(statsList);

            // save stats to store
            addHistogram(statsList.get(0).getHistogramList());
            addCountMinSketch(statsList.get(0).getCountMinSketchList());
            addStatsNormal(statsList.get(0).getStatsNormalList());
            // update analyze job status
            cache(statsList.get(0));
            log.info("stats collect done");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static List<CompletableFuture<TableStats>> getCompletableFutures(TableDefinition td,
                                                                             CommonId tableId,
                                                                             List<RangeDistribution> rangeDistributions,
                                                                             List<CountMinSketch> cmSketchList,
                                                                             List<StatsNormal> statsNormals,
                                                                             List<Histogram> columnHistograms) {
        List<CompletableFuture<TableStats>> futureList = rangeDistributions.stream().map(_i -> {
            Callable<TableStats> collectStatsTask = new CollectStatsTask(_i, tableId, td,
                columnHistograms,
                cmSketchList,
                statsNormals);
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

    private void typeMetricAdaptor(TableDefinition td,
                                   List<Histogram> histogramCdList,
                                   List<CountMinSketch> cmSketchCdList,
                                   List<StatsNormal> statsNormals,
                                   int cmSketchWidth,
                                   int cmSketchHeight) {
        AtomicInteger index = new AtomicInteger();
        td.getColumns().forEach(columnDefinition -> {
            index.incrementAndGet();
            if (columnList.size() > 0 && !columnList.contains(columnDefinition.getName())) {
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
                    cmSketchWidth, cmSketchHeight, totalCount));
            }
            if (!allowStats) {
                return;
            }
            statsNormals.add(new StatsNormal(
                columnDefinition.getName(),
                totalCount,
                columnDefinition.getType()));
        });
    }

    private void addHistogram(List<Histogram> histogramList) {
        deletePrefix(bucketsStore, bucketsCodec, new Object[]{schemaName, tableName, null, null});
        List<Object[]> paramList = histogramList.stream().map(histogram -> {
            String histogramDetail = histogram.serialize();
            return new Object[] {histogram.getSchemaName(), histogram.getTableName(), histogram.getColumnName(),
                histogramDetail};
        }).collect(Collectors.toList());
        insert(bucketsStore, bucketsCodec, paramList);
    }

    private void addCountMinSketch(List<CountMinSketch> countMinSketches) {
        deletePrefix(cmSketchStore, cmSketchCodec, new Object[]{schemaName, tableName, null, null, null, null});
        List<Object[]> paramList = countMinSketches.stream().map(countMinSketch -> {
            String cmSketch = countMinSketch.serialize();
            return new Object[] {countMinSketch.getSchemaName(), countMinSketch.getTableName(),
                countMinSketch.getColumnName(), cmSketch, countMinSketch.getNullCount(), countMinSketch.getTotalCount()
            };
        }).collect(Collectors.toList());
        insert(cmSketchStore, cmSketchCodec, paramList);
    }

    private void addStatsNormal(List<StatsNormal> statsNormals) {
        deletePrefix(statsStore, statsCodec, new Object[]{schemaName, tableName, null, null, null, null, null});
        List<Object[]> paramList = statsNormals.stream().map(statsNormal ->
            new Object[] {schemaName, tableName, statsNormal.getColumnName(), statsNormal.getNdv(),
                statsNormal.getNumNull(), statsNormal.getAvgColSize(), statsNormal.getTotalCount()}
        ).collect(Collectors.toList());
        insert(statsStore, statsCodec, paramList);
    }

    private static void cache(TableStats tableStats) {
        StatsCache.statsMap.put(tableStats.getIdentifier(), tableStats);
    }

    private void buildHistogram(List<Histogram> histogramList,
                                          List<RangeDistribution> rangeDistributions,
                                          CommonId tableId,
                                          TableDefinition td) {
        if (histogramList.size() > 0) {
            List<Iterator<Object[]>> iteratorList = rangeDistributions.stream().map(rangeDistribution -> {
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
                    td.getDingoType(), td.getKeyMapping(), tableId.seq));
                builder.resultSchema(SchemaWrapperUtils.buildSchemaWrapper(
                    outputSchema, outputKeyMapping, tableId.seq
                ));
                Coprocessor coprocessor = builder.build();
                Part part = new PartInKvStore(Services.KV_STORE.getInstance(tableId, rangeDistribution.getId()),
                    CodecService.getDefault().createKeyValueCodec(tableId, outputSchema, outputKeyMapping));
                return part.scan(rangeDistribution.getStartKey(), rangeDistribution.getEndKey(),
                    true, false, coprocessor);
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

}
