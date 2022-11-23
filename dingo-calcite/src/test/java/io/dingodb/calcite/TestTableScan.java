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

package io.dingodb.calcite;

import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.calcite.rel.DingoPartRangeScan;
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rel.DingoStreamingConverter;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.LogicalDingoRoot;
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.impl.JobManagerImpl;
import io.dingodb.test.asserts.Assert;
import io.dingodb.test.asserts.AssertJob;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTableScan {
    private static final JobManager jobManager = JobManagerImpl.INSTANCE;
    private static DingoParserContext context;
    private static Location currentLocation;
    private static int tableTestPartNum;
    private DingoParser parser;

    @BeforeAll
    public static void setupAll() {
        MockMetaServiceProvider.init();
        context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
        DingoSchema dingoSchema = (DingoSchema) context.getDefaultSchema().schema;
        currentLocation = dingoSchema.getMetaService().currentLocation();
        tableTestPartNum = dingoSchema.getMetaService().getParts(MockMetaServiceProvider.TABLE_NAME).size();
    }

    @BeforeEach
    public void setup() {
        // Create each time to clean the statistic info.
        parser = new DingoParser(context);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "select * from test",
        "select * from TeST",
        "select * from dingo.test",
    })
    public void testScan(String sql) throws SqlParseException {
        // To logical plan.
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        // To physical plan.
        RelNode relNode = parser.optimize(relRoot.rel);
        DingoTableScan scan = (DingoTableScan) Assert.relNode(relNode)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class)
            .getInstance();
        assertThat((scan).getFilter()).isNull();
        assertThat((scan).getSelection()).isNull();
        // To job.
        Job job = jobManager.createJob(Id.random());
        DingoJobVisitor.renderJob(job, relNode, currentLocation);
        AssertJob assertJob = Assert.job(job).taskNum(tableTestPartNum);
        assertJob.task("0001").location(currentLocation).operatorNum(4);
    }

    @Test
    public void testFilterScan() throws SqlParseException {
        String sql = "select * from test where name = 'Alice' and amount > 3.0";
        // To logical plan.
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        // To physical plan.
        RelNode relNode = parser.optimize(relRoot.rel);
        DingoTableScan scan = (DingoTableScan) Assert.relNode(relNode)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class)
            .getInstance();
        assertThat((scan).getFilter()).isNotNull();
        assertThat((scan).getSelection()).isNull();
        // To job.
        Job job = jobManager.createJob(Id.random());
        DingoJobVisitor.renderJob(job, relNode, currentLocation);
        AssertJob assertJob = Assert.job(job).taskNum(tableTestPartNum);
        assertJob.task("0001").location(currentLocation).operatorNum(4);
    }

    @Test
    public void testProjectScan() throws SqlParseException {
        String sql = "select name, amount from test";
        // To logical plan.
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        // To physical plan.
        RelNode relNode = parser.optimize(relRoot.rel);
        DingoTableScan scan = (DingoTableScan) Assert.relNode(relNode)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class)
            .getInstance();
        assertThat((scan).getFilter()).isNull();
        assertThat((scan).getSelection()).isNotNull();
        // To job.
        Job job = jobManager.createJob(Id.random());
        DingoJobVisitor.renderJob(job, relNode, currentLocation);
        AssertJob assertJob = Assert.job(job).taskNum(tableTestPartNum);
        assertJob.task("0001").location(currentLocation).operatorNum(4);
    }

    @Test
    public void testProjectFilterScan() throws SqlParseException {
        String sql = "select name, amount from test where amount > 3.0";
        // To logical plan.
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        // To physical plan.
        RelNode relNode = parser.optimize(relRoot.rel);
        DingoTableScan scan = (DingoTableScan) Assert.relNode(relNode)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class)
            .getInstance();
        assertThat((scan).getFilter()).isNotNull();
        assertThat((scan).getSelection()).isNotNull();
        // To job.
        Job job = jobManager.createJob(Id.random());
        DingoJobVisitor.renderJob(job, relNode, currentLocation);
        AssertJob assertJob = Assert.job(job).taskNum(tableTestPartNum);
        assertJob.task("0001").location(currentLocation).operatorNum(4);
    }

    @Test
    public void testFilterScanWithParameters() throws SqlParseException {
        String sql = "select * from test where name = ? and amount > ?";
        // To logical plan.
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        Assert.relNode(relRoot.rel)
            .isA(LogicalDingoRoot.class)
            .soleInput().isA(LogicalProject.class)
            .soleInput().isA(LogicalFilter.class)
            .soleInput().isA(LogicalDingoTableScan.class);
        // To physical plan.
        RelNode relNode = parser.optimize(relRoot.rel);
        DingoTableScan scan = (DingoTableScan) Assert.relNode(relNode)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoTableScan.class)
            .getInstance();
        assertThat((scan).getFilter()).isNotNull();
        assertThat((scan).getSelection()).isNull();
        // To job.
        Job job = jobManager.createJob(Id.random());
        DingoJobVisitor.renderJob(job, relNode, currentLocation);
        AssertJob assertJob = Assert.job(job).taskNum(tableTestPartNum);
        assertJob.task("0001").location(currentLocation).operatorNum(4);
    }

    @Test
    public void testBetweenAnd() throws SqlParseException {
        String sql = "select * from test where id between 2 and 5";
        SqlNode sqlNode = parser.parse(sql);
        RelRoot relRoot = parser.convert(sqlNode);
        RelNode optimized = parser.optimize(relRoot.rel);
        RelNode relNode = Assert.relNode(optimized)
            .isA(DingoRoot.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoStreamingConverter.class).streaming(DingoRelStreaming.ROOT)
            .soleInput().isA(DingoPartRangeScan.class)
            .getInstance();
        DingoPartRangeScan scan = (DingoPartRangeScan) relNode;
        assertThat((scan).getFilter()).isNotNull();
        assertThat((scan).getSelection()).isNull();
    }
}
