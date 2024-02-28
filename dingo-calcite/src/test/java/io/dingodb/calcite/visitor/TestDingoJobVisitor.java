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

package io.dingodb.calcite.visitor;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoParser;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.calcite.rel.dingo.DingoStreamingConverter;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.calcite.schema.DingoSchema;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.common.Location;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.impl.JobManagerImpl;
import io.dingodb.exec.operator.ValuesOperator;
import io.dingodb.exec.operator.params.ValuesParam;
import io.dingodb.test.asserts.Assert;
import io.dingodb.test.asserts.AssertJob;
import io.dingodb.test.asserts.AssertTask;
import io.dingodb.tso.TsoService;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDingoJobVisitor {
    private static final String FULL_TABLE_NAME = MockMetaServiceProvider.TABLE_NAME;
    private static final JobManager jobManager = JobManagerImpl.INSTANCE;
    private static DingoParserContext context;

    private static DingoParser parser;
    private static Location currentLocation;
    private static RelOptTable table;
    private static RelDataType rowType;

    @BeforeAll
    public static void setupAll() {
        MockMetaServiceProvider.init();
        context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
    }

    @BeforeEach
    public void setup() {
        parser = new DingoParser(context);
        DingoSchema dingoSchema = (DingoSchema) context.getDefaultSchema().schema;
        currentLocation = dingoSchema.getMetaService().currentLocation();
        table = context.getCatalogReader().getTable(ImmutableList.of(FULL_TABLE_NAME));
        RelDataTypeFactory typeFactory = parser.getContext().getTypeFactory();
        rowType = typeFactory.createStructType(
            ImmutableList.of(
                typeFactory.createSqlType(SqlTypeName.INTEGER),
                typeFactory.createSqlType(SqlTypeName.VARCHAR, 64),
                typeFactory.createSqlType(SqlTypeName.DOUBLE)
            ),
            ImmutableList.of(
                "id",
                "name",
                "amount"
            )
        );
    }

    @Test
    public void testVisitTableScan() {
        DingoTableScan scan = new DingoTableScan(
            parser.getCluster(),
            parser.getPlanner().emptyTraitSet()
                .replace(DingoConvention.INSTANCE)
                .replace(DingoRelStreaming.of(table)),
            ImmutableList.of(),
            table,
            null,
            null
        );
        long jobSeqId = TsoService.getDefault().tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId);
        DingoJobVisitor.renderJob(job, scan, currentLocation);
        AssertJob assertJob = Assert.job(job).taskNum(1);
        /*CommonId tableId = MetaService.root()
            .getSubMetaService(DingoRootSchema.DEFAULT_SCHEMA_NAME)
            .getTableId(FULL_TABLE_NAME);*/
        assertJob.task(jobSeqId, 0).operatorNum(2).location(MockMetaServiceProvider.LOC_0)
            .source(0).isCalcDistribution().outputNum(1);
        // assertJob.task(jobSeqId, 0).operatorNum(3).location(MockMetaServiceProvider.LOC_0)
        //    .source(0).isPartRangeScan(tableId, new CommonId(DISTRIBUTION, tableId.seq, 1))
        //    .soleOutput().isNull();
        // assertJob.task(jobSeqId, 0).operatorNum(3).location(MockMetaServiceProvider.LOC_1)
        //    .source(1).isPartRangeScan(tableId, new CommonId(DISTRIBUTION, tableId.seq, 2))
        //    .soleOutput().isNull();
    }

    @Test
    public void testVisitDingoStreamingConverterNotRoot() {
        DingoStreamingConverter converter = new DingoStreamingConverter(
            parser.getCluster(),
            parser.getPlanner().emptyTraitSet()
                .replace(DingoConvention.INSTANCE)
                .replace(DingoRelStreaming.of(table).changeDistribution(null)),
            new DingoTableScan(
                parser.getCluster(),
                parser.getPlanner().emptyTraitSet()
                    .replace(DingoConvention.INSTANCE)
                    .replace(DingoRelStreaming.of(table)),
                ImmutableList.of(),
                table,
                null,
                null
            )
        );
        long jobSeqId = TsoService.getDefault().tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId);
        DingoJobVisitor.renderJob(job, converter, currentLocation);
        AssertJob assertJob = Assert.job(job).taskNum(1);
        AssertTask assertTask =
            assertJob.task(jobSeqId, 0).operatorNum(2).location(MockMetaServiceProvider.LOC_0).sourceNum(1);
        assertJob.task(jobSeqId, 0).operatorNum(2).location(MockMetaServiceProvider.LOC_0).sourceNum(1);
        assertTask.source(0).isCalcDistribution().outputNum(1);
        /*CommonId tableId = MetaService.root()
            .getSubMetaService(DingoRootSchema.DEFAULT_SCHEMA_NAME)
            .getTableId(FULL_TABLE_NAME);*/
        // assertTask.source(0).isPartRangeScan(tableId, new CommonId(DISTRIBUTION, tableId.seq, 1))
        //    .soleOutput().isNull();
        // assertTask.source(1).isPartRangeScan(tableId, new CommonId(DISTRIBUTION, tableId.seq, 2))
        //    .soleOutput().isNull();
        //assertJob.task("0003").operatorNum(2).location(MockMetaServiceProvider.LOC_1)
        //    .soleSource().isPartScan(tableId, new CommonId((byte) 'T', tableId.seq, 2))
        //    .soleOutput().isA(SendOperator.class);
    }

    @Test
    public void testVisitDingoStreamingConverterRoot() {
        DingoStreamingConverter converter = new DingoStreamingConverter(
            parser.getCluster(),
            parser.getPlanner().emptyTraitSet()
                .replace(DingoConvention.INSTANCE)
                .replace(DingoRelStreaming.ROOT),
            new DingoTableScan(
                parser.getCluster(),
                parser.getPlanner().emptyTraitSet()
                    .replace(DingoConvention.INSTANCE)
                    .replace(DingoRelStreaming.of(table)),
                ImmutableList.of(),
                table,
                null,
                null
            )
        );
        long jobSeqId = TsoService.getDefault().tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId);
        DingoJobVisitor.renderJob(job, converter, currentLocation);
        AssertJob assertJob = Assert.job(job).taskNum(1);
        AssertTask assertTask =
            assertJob.task(jobSeqId, 0).operatorNum(2).location(MockMetaServiceProvider.LOC_0).sourceNum(1);
        assertJob.task(jobSeqId, 0).operatorNum(2).location(MockMetaServiceProvider.LOC_0).sourceNum(1);
        assertTask.source(0).isCalcDistribution().outputNum(1);
        /*CommonId tableId = MetaService.root()
            .getSubMetaService(DingoRootSchema.DEFAULT_SCHEMA_NAME)
            .getTableId(FULL_TABLE_NAME);*/
        // assertTask.source(0).isPartRangeScan(tableId, new CommonId(DISTRIBUTION, tableId.seq, 1))
        //    .soleOutput().isA(CoalesceOperator.class);
        // assertTask.source(1).isPartRangeScan(tableId, new CommonId(DISTRIBUTION, tableId.seq, 2))
        //    .soleOutput().isA(CoalesceOperator.class);
        //assertJob.task("0003").operatorNum(2).location(MockMetaServiceProvider.LOC_1)
        //    .soleSource().isPartScan(tableId, new CommonId((byte) 'T', tableId.seq, 2))
        //    .soleOutput().isA(SendOperator.class);
    }

    @Test
    public void testVisitValues() {
        DingoValues values = new DingoValues(
            parser.getCluster(),
            parser.getPlanner().emptyTraitSet()
                .replace(DingoConvention.INSTANCE)
                .replace(DingoRelStreaming.ROOT),
            rowType,
            ImmutableList.of(
                new Object[]{1, "Alice", 1.0},
                new Object[]{2, "Betty", 2.0}
            )
        );
        long jobSeqId = TsoService.getDefault().tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId);
        DingoJobVisitor.renderJob(job, values, currentLocation);
        Vertex vertex = Assert.job(job)
            .soleTask().location(MockMetaServiceProvider.LOC_0).operatorNum(1)
            .soleSource().isA(ValuesOperator.class)
            .getVertex();
        ValuesParam param = (ValuesParam) vertex.getData();

        List<Object[]> tuples = param.getTuples();
        assertThat(tuples).element(0).satisfies(obj -> {
            assertThat(obj[0]).isEqualTo(1);
            assertThat(obj[1]).isEqualTo("Alice");
            assertThat(obj[2]).isEqualTo(1.0);
        });
    }

    @Test
    public void testVisitPartModify() {
        RelOptCluster cluster = parser.getCluster();
        DingoTableModify partModify = new DingoTableModify(
            cluster,
            cluster.traitSetOf(DingoConvention.INSTANCE),
            table,
            context.getCatalogReader(),
            new DingoTableScan(
                parser.getCluster(),
                parser.getPlanner().emptyTraitSet()
                    .replace(DingoConvention.INSTANCE)
                    .replace(DingoRelStreaming.of(table)),
                ImmutableList.of(),
                table,
                null,
                null
            ),
            TableModify.Operation.INSERT,
            null,
            null,
            true
        );
        long jobSeqId = TsoService.getDefault().tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId);
        DingoJobVisitor.renderJob(job, partModify, currentLocation);
        Assert.job(job).taskNum(1)
            .task(jobSeqId, 0).location(MockMetaServiceProvider.LOC_0).operatorNum(3);
    }
}
