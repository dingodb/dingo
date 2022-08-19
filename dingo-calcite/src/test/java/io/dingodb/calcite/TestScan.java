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

import io.dingodb.calcite.assertion.Assert;
import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoExchangeRoot;
import io.dingodb.calcite.rel.DingoPartScan;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.exec.base.Job;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestScan {
    private static DingoParser parser;
    private static Location currentLocation;
    private static int tableTestPartNum;

    @BeforeAll
    public static void setupAll() {
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
        parser = new DingoParser(context);
        DingoSchema dingoSchema = (DingoSchema) context.getDefaultSchema().schema;
        currentLocation = dingoSchema.getMetaService().currentLocation();
        tableTestPartNum = dingoSchema.getMetaService().getParts("test").size();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "select * from test",
        "select * from TeST",
        "select * from mock.test",
    })
    public void testScan(String sql) throws SqlParseException {
        // To logical plan.
        RelRoot relRoot = parser.parseRel(sql);
        Assert.relNode(relRoot.rel).isA(LogicalProject.class).convention(Convention.NONE)
            .singleInput().isA(DingoTableScan.class).convention(Convention.NONE);
        // To physical plan.
        RelNode relNode = parser.optimize(relRoot.rel);
        DingoPartScan scan = (DingoPartScan) Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat((scan).getFilter()).isNull();
        assertThat((scan).getSelection()).isNull();
        // To job.
        Job job = DingoJobVisitor.createJob(relNode, currentLocation);
        Assert.job(job).taskNum(tableTestPartNum)
            .task("0001", t -> t.location(currentLocation).operatorNum(3));
    }

    @Test
    public void testFilterScan() throws SqlParseException {
        String sql = "select * from test where name = 'Alice' and amount > 3.0";
        // To logical plan.
        RelRoot relRoot = parser.parseRel(sql);
        Assert.relNode(relRoot.rel).isA(LogicalProject.class).convention(Convention.NONE)
            .singleInput().isA(LogicalFilter.class).convention(Convention.NONE)
            .singleInput().isA(DingoTableScan.class).convention(Convention.NONE);
        // To physical plan.
        RelNode relNode = parser.optimize(relRoot.rel);
        DingoPartScan scan = (DingoPartScan) Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat((scan).getFilter()).isNotNull();
        assertThat((scan).getSelection()).isNull();
        // To job.
        Job job = DingoJobVisitor.createJob(relNode, currentLocation);
        Assert.job(job).taskNum(tableTestPartNum)
            .task("0001", t -> t.location(currentLocation).operatorNum(3));
    }

    @Test
    public void testProjectScan() throws SqlParseException {
        String sql = "select name, amount from test";
        // To logical plan.
        RelRoot relRoot = parser.parseRel(sql);
        Assert.relNode(relRoot.rel).isA(LogicalProject.class).convention(Convention.NONE)
            .singleInput().isA(DingoTableScan.class).convention(Convention.NONE);
        // To physical plan.
        RelNode relNode = parser.optimize(relRoot.rel);
        DingoPartScan scan = (DingoPartScan) Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat((scan).getFilter()).isNull();
        assertThat((scan).getSelection()).isNotNull();
        // To job.
        Job job = DingoJobVisitor.createJob(relNode, currentLocation);
        Assert.job(job).taskNum(tableTestPartNum)
            .task("0001", t -> t.location(currentLocation).operatorNum(3));
    }

    @Test
    public void testProjectFilterScan() throws SqlParseException {
        String sql = "select name, amount from test where amount > 3.0";
        // To logical plan.
        RelRoot relRoot = parser.parseRel(sql);
        Assert.relNode(relRoot.rel).isA(LogicalProject.class).convention(Convention.NONE)
            .singleInput().isA(LogicalFilter.class).convention(Convention.NONE)
            .singleInput().isA(DingoTableScan.class).convention(Convention.NONE);
        // To physical plan.
        RelNode relNode = parser.optimize(relRoot.rel);
        DingoPartScan scan = (DingoPartScan) Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat((scan).getFilter()).isNotNull();
        assertThat((scan).getSelection()).isNotNull();
        // To job.
        Job job = DingoJobVisitor.createJob(relNode, currentLocation);
        Assert.job(job).taskNum(tableTestPartNum)
            .task("0001", t -> t.location(currentLocation).operatorNum(3));
    }

    @Test
    public void testFilterScanWithParameters() throws SqlParseException {
        String sql = "select * from test where name = ? and amount > ?";
        // To logical plan.
        RelRoot relRoot = parser.parseRel(sql);
        Assert.relNode(relRoot.rel).isA(LogicalProject.class).convention(Convention.NONE)
            .singleInput().isA(LogicalFilter.class).convention(Convention.NONE)
            .singleInput().isA(DingoTableScan.class).convention(Convention.NONE);
        // To physical plan.
        RelNode relNode = parser.optimize(relRoot.rel);
        DingoPartScan scan = (DingoPartScan) Assert.relNode(relNode)
            .isA(DingoCoalesce.class).convention(DingoConventions.ROOT)
            .singleInput().isA(DingoExchangeRoot.class).convention(DingoConventions.PARTITIONED)
            .singleInput().isA(DingoPartScan.class).convention(DingoConventions.DISTRIBUTED)
            .getInstance();
        assertThat((scan).getFilter()).isNotNull();
        assertThat((scan).getSelection()).isNull();
        // To job.
        Job job = DingoJobVisitor.createJob(relNode, currentLocation);
        Assert.job(job).taskNum(tableTestPartNum)
            .task("0001", t -> t.location(currentLocation).operatorNum(3));
    }
}
