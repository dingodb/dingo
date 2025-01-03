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
import com.google.common.collect.Iterators;
import io.dingodb.calcite.stats.AnalyzeInfo;
import io.dingodb.calcite.stats.CountMinSketch;
import io.dingodb.calcite.stats.Histogram;
import io.dingodb.calcite.stats.StatsCache;
import io.dingodb.calcite.stats.StatsNormal;
import io.dingodb.calcite.stats.StatsOperator;
import io.dingodb.calcite.stats.StatsTaskState;
import io.dingodb.calcite.stats.TableStats;
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.CoprocessorV2;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
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
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.expr.DingoCompileContext;
import io.dingodb.exec.expr.DingoRelConfig;
import io.dingodb.exec.utils.SchemaWrapperUtils;
import io.dingodb.expr.coding.CodingFlag;
import io.dingodb.expr.coding.RelOpCoder;
import io.dingodb.expr.common.type.TupleType;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.tso.TsoService;
import lombok.Builder;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

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
            long start = System.currentTimeMillis();
            // get table info
            MetaService metaService = MetaService.root();
            metaService = metaService.getSubMetaService(schemaName);
            if (metaService == null) {
                return;
            }
            Table td = DdlService.root().getTable(schemaName, tableName);
            if (td == null) {
                return;
            }
            CommonId tableId = td.getTableId();

            startAnalyzeTask(tableId);
            PartitionService ps = PartitionService.getService(
                Optional.ofNullable(td.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));

            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistributionNavigableMap
                = metaService.getRangeDistribution(tableId);

            Set<RangeDistribution> distributions
                = ps.calcPartitionRange(null, null, true, true,
                rangeDistributionNavigableMap);

            List<Histogram> histogramList = new ArrayList<>();
            List<CountMinSketch> cmSketchList = new ArrayList<>();
            List<StatsNormal> statsNormals = new ArrayList<>();
            long end1 = System.currentTimeMillis();

            // varchar -> count-min-sketch  int,float,double,date,time,timestamp -> histogram
            // ndv, nullCount -> normal
            typeMetricAdaptor(td, histogramList, cmSketchList, statsNormals, cmSketchWidth, cmSketchHeight);
            // par scan get min, max
            // histogram equ-width need max, min
            try {
                buildHistogram(histogramList, distributions, tableId, td);
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
                //histogramList.clear();
            }

            long end2 = System.currentTimeMillis();
            LogUtils.info(log, "init type cost:{}", (end2 - end1));
            List<TableStats> statsList = null;
            try {
                List<CompletableFuture<TableStats>> futureList = getCompletableFutures(td, tableId, distributions,
                    cmSketchList, statsNormals, histogramList);

                LogUtils.info(log, "get futureList...");
                statsList = futureList.stream().map(CompletableFuture::toCompletableFuture)
                    .map(o -> {
                        try {
                            return o.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList());
            } catch (Exception e) {
                failReason = e.getMessage();
                LogUtils.error(log, e.getMessage(), e);
            }
            // merge regions stats
            if (statsList == null) {
                return;
            }
            long end3 = System.currentTimeMillis();
            LogUtils.info(log, "build stats cost:{}", (end3 - end2));
            TableStats.mergeStats(statsList);
            TableStats tableStats = statsList.get(0);
            long end4 = System.currentTimeMillis();
            LogUtils.info(log, "stats merge success cost:{}", (end4 - end3));

            // save stats to store
            addHistogram(tableStats.getHistogramList());
            long end5 = System.currentTimeMillis();
            LogUtils.info(log, "add histogram cost:{}", (end5 - end4));
            addCountMinSketch(tableStats.getCountMinSketchList());
            long end6 = System.currentTimeMillis();
            LogUtils.info(log, "add count min sketch cost:{}", (end6 - end5));
            addStatsNormal(tableStats.getStatsNormalList());
            long end7 = System.currentTimeMillis();
            LogUtils.info(log, "add stats normal cost:{}", (end7 - end6));
            // update analyze job status
            cache(tableStats);
            rowCount = tableStats.getRowCount();
            long end = System.currentTimeMillis();
            LogUtils.info(log, "stats collect done, take time:{}, tableName:{}, rowCount:{}",
                (end - start), tableName, rowCount);
        } catch (Exception e) {
            failReason = e.getMessage();
            LogUtils.error(log, e.getMessage(), e);
        }
        endAnalyzeTask(failReason, rowCount);
    }

    private List<CompletableFuture<TableStats>> getCompletableFutures(
        Table td,
        CommonId tableId,
        Set<RangeDistribution> rangeDistributions,
        List<CountMinSketch> cmSketchList,
        List<StatsNormal> statsNormals,
        List<Histogram> columnHistograms
    ) {
        long scanTs = TsoService.getDefault().cacheTso();

        return rangeDistributions.stream().map(_i -> {
            Callable<TableStats> collectStatsTask = new CollectStatsTask(
                _i, tableId, td, columnHistograms, cmSketchList, statsNormals, scanTs, timeout
            );
            return Executors.submit("collect-task", collectStatsTask);
        }).collect(Collectors.toList());
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
            if (columnList != null && !columnList.isEmpty() && !columnList.contains(columnDefinition.getName())) {
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
                histogramDetail, new Timestamp(System.currentTimeMillis())};
        }).collect(Collectors.toList());
        upsert(bucketsStore, bucketsCodec, paramList);
    }

    private void addCountMinSketch(List<CountMinSketch> countMinSketches) {
        List<Object[]> paramList = countMinSketches.stream().map(countMinSketch -> {
            String cmSketch = countMinSketch.serialize();
            return new Object[] {countMinSketch.getSchemaName(), countMinSketch.getTableName(),
                countMinSketch.getColumnName(), cmSketch, countMinSketch.getNullCount(),
                countMinSketch.getTotalCount(), countMinSketch.getIndex(), new Timestamp(System.currentTimeMillis())
            };
        }).collect(Collectors.toList());
        long start = System.currentTimeMillis();
        upsert(cmSketchStore, cmSketchCodec, paramList);
        long end = System.currentTimeMillis();
        LogUtils.info(log, "add sketch done, take time:{}", (end - start));
    }

    private void addStatsNormal(List<StatsNormal> statsNormals) {
        List<Object[]> paramList = statsNormals.stream().map(statsNormal ->
            new Object[] {schemaName, tableName, statsNormal.getColumnName(), statsNormal.getNdv(),
                statsNormal.getNumNull(), statsNormal.getAvgColSize(), statsNormal.getTotalCount(),
                new Timestamp(System.currentTimeMillis())
            }
        ).collect(Collectors.toList());
        upsert(statsStore, statsCodec, paramList);
    }

    private static void cache(TableStats tableStats) {
        tableStats.initRowCount();
        StatsCache.statsMap.put(tableStats.getIdentifier(), tableStats);
    }

    private void buildHistogram(List<Histogram> histogramList,
                                Set<RangeDistribution> rangeDistributions,
                                CommonId tableId,
                                Table td) {
        if (histogramList.isEmpty()) {
            return;
        }
        List<Iterator<Object[]>> iteratorList = rangeDistributions.stream().map(region -> {
            DingoType outputSchema = DingoTypeFactory.tuple(
                histogramList.stream().flatMap(histogram ->
                    Arrays.stream(new DingoType[]{histogram.getDingoType(),
                        histogram.getDingoType()})).toArray(DingoType[]::new));
            CoprocessorV2 coprocessor = getCoprocessor(td, histogramList, outputSchema);
            if (coprocessor == null) {
                return null;
            }
            byte[] startKey = region.getStartKey();
            byte[] endKey = region.getEndKey();
            CodecService.getDefault().setId(startKey, region.getId().domain);
            CodecService.getDefault().setId(endKey, region.getId().domain);
            StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, region.getId());

            Iterator<KeyValue> iterator = kvStore.txnScan(
                TsoService.getDefault().cacheTso(),
                new StoreInstance.Range(startKey, endKey,
                   region.isWithStart(), region.isWithEnd()),
                30000,
                coprocessor
            );
            TupleMapping outputKeyMapping = TupleMapping.of(
                IntStream.range(0, 0).boxed().collect(Collectors.toList())
            );
            return Iterators.transform(
                iterator,
                wrap(CodecService.getDefault().createKeyValueCodec(td.getCodecVersion(), td.version,
                   outputSchema, outputKeyMapping)::decode)::apply
            );
        }).collect(Collectors.toList());
        for (Iterator<Object[]> iterator : iteratorList) {
            if (iterator == null) {
                continue;
            }
            while (iterator.hasNext()) {
                Object[] tuples = iterator.next();
                for (int i = 0; i < histogramList.size(); i ++) {
                    histogramList.get(i).setRegionMax(tuples[2 * i]);
                    histogramList.get(i).setRegionMin(tuples[2 * i + 1]);
                }
            }
        }
        histogramList.forEach(histogram -> histogram.init(bucketCount));
    }

    private void startAnalyzeTask(CommonId tableId) {
        Object[] values = get(analyzeTaskStore, analyzeTaskCodec, getAnalyzeTaskKeys(schemaName, tableName));
        if (values == null) {
            Long commitCount = 0L;
            try {
                commitCount = MetaService.root().getTableCommitCount().getOrDefault(tableId, 0L);
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
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
            LogUtils.error(log, e.getMessage(), e);
        }
    }

    private void endAnalyzeTask(String failReason, long rowCount) {
        Object[] values = get(analyzeTaskStore, analyzeTaskCodec, getAnalyzeTaskKeys(schemaName, tableName));
        if (values == null) {
            LogUtils.error(log, "analyze task is null");
            return;
        }
        long now = System.currentTimeMillis();
        Timestamp current = new Timestamp(now);
        Long modify = 0L;
        Long execCount = (Long) values[13];
        long totalExecCount = execCount + 1;
        values[13] = totalExecCount;
        if (StringUtils.isBlank(failReason)) {
            modify = (Long) values[9];
            if (rowCount == 0) {
                if (totalExecCount < 20) {
                    values[6] = StatsTaskState.PENDING.getState();
                } else {
                    delStats("mysql.analyze_task", schemaName, tableName);
                }
            } else {
                values[6] = StatsTaskState.SUCCESS.getState();
            }
        } else {
            values[6] = StatsTaskState.FAIL.getState();
            values[7] = failReason;
        }
        values[5] = current;
        values[9] = 0L;
        values[10] = current;
        values[3] = rowCount;
        Long lastModifyCount = (Long) values[11];
        values[11] = lastModifyCount + modify;
        Timestamp startTime = (Timestamp) values[4];
        long duration = now - startTime.getTime();
        values[12] = duration;
        long lastExecTime = (long) values[14];
        values[14] = lastExecTime + duration;
        upsert(analyzeTaskStore, analyzeTaskCodec, Collections.singletonList(values));
    }

    private String getAnalyzeParam() {
        AnalyzeInfo analyzeInfo = new AnalyzeInfo(cmSketchHeight, cmSketchWidth, bucketCount, columnList);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(analyzeInfo);
        } catch (JsonProcessingException e) {
            LogUtils.error(log, e.getMessage(), e);
            return "";
        }
    }

    public static CoprocessorV2 getCoprocessor(Table table, List<Histogram> histograms, DingoType outputSchema) {
        CommonId tableId = table.tableId;
        List<DingoType> dingoTypes = table.getColumns().stream().map(Column::getType).collect(Collectors.toList());
        DingoType schema = DingoTypeFactory.tuple(dingoTypes.toArray(new DingoType[]{}));

        DingoRelConfig config = new DingoRelConfig();
        Expr[] exprs = histograms.stream()
            .flatMap(histogram ->
                Arrays.stream(new Expr[]{makeMaxAgg(histogram.getIndex()), makeMinAgg(histogram.getIndex())})
            ).toArray(Expr[]::new);
        RelOp relOp = RelOpBuilder.builder().agg(exprs).build();

        relOp = relOp.compile(new DingoCompileContext(
            (TupleType) schema.getType(),
            (TupleType) DingoTypeFactory.tuple(new DingoType[0]).getType()
        ), config);

        CoprocessorV2 coprocessor;
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        if (RelOpCoder.INSTANCE.visit(relOp, os) == CodingFlag.OK) {
            List<Integer> selection = IntStream.range(0, schema.fieldCount())
                .boxed()
                .collect(Collectors.toList());
            TupleMapping outputKeyMapping = TupleMapping.of(new int[]{});
            coprocessor = CoprocessorV2.builder()
                .originalSchema(SchemaWrapperUtils.buildSchemaWrapper(schema, table.keyMapping(), tableId.seq))
                .resultSchema(SchemaWrapperUtils.buildSchemaWrapper(outputSchema, outputKeyMapping, tableId.seq))
                .selection(selection)
                .relExpr(os.toByteArray())
                .schemaVersion(table.getVersion())
                .build();
            return coprocessor;
        }
        return null;
    }

    private static Expr makeMaxAgg(int index) {
        Expr var = DingoCompileContext.createTupleVar(index);
        return Exprs.op(Exprs.MAX_AGG, var);
    }

    private static Expr makeMinAgg(int index) {
        Expr var = DingoCompileContext.createTupleVar(index);
        return Exprs.op(Exprs.MIN_AGG, var);
    }
}
