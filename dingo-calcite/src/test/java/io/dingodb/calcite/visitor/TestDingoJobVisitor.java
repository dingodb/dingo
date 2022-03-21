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
import io.dingodb.calcite.DingoConventions;
import io.dingodb.calcite.DingoParser;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.assertion.Assert;
import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoDistributedValues;
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoPartScan;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.common.table.TableId;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.operator.CoalesceOperator;
import io.dingodb.exec.operator.PartModifyOperator;
import io.dingodb.exec.operator.ReceiveOperator;
import io.dingodb.exec.operator.SendOperator;
import io.dingodb.exec.operator.ValuesOperator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDingoJobVisitor {
    private static final String FULL_TABLE_NAME = MockMetaServiceProvider.TABLE_NAME;
    private static final TableId TABLE_ID = new TableId(FULL_TABLE_NAME.getBytes(StandardCharsets.UTF_8));

    private static DingoParser parser;
    private static RelOptTable table;
    private static DingoValues values;
    private static DingoDistributedValues distributedValues;

    @BeforeAll
    public static void setupAll() {
        parser = new DingoParser(new DingoParserContext());
        table = parser.getCatalogReader().getTable(ImmutableList.of(FULL_TABLE_NAME));
        RelOptCluster cluster = parser.getCluster();
        RelDataTypeFactory typeFactory = parser.getContext().getTypeFactory();
        RelDataType rowType = typeFactory.createStructType(
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
        values = new DingoValues(
            cluster,
            rowType,
            ImmutableList.of(
                new Object[]{1, "Alice", 1.0},
                new Object[]{2, "Betty", 2.0}
            ),
            cluster.traitSetOf(DingoConventions.ROOT)
        );
        distributedValues = new DingoDistributedValues(
            cluster,
            rowType,
            values.getTuples(),
            cluster.traitSetOf(DingoConventions.DISTRIBUTED),
            table
        );
    }

    @Test
    public void testVisitPartScan() {
        RelOptCluster cluster = parser.getCluster();
        DingoPartScan partScan = new DingoPartScan(
            cluster,
            cluster.traitSetOf(DingoConventions.DISTRIBUTED),
            table
        );
        Job job = DingoJobVisitor.createJob(partScan);
        Assert.job(job).taskNum(2)
            .task(0, t -> t.operatorNum(1).location(MockMetaServiceProvider.LOC_0)
                .soleSource().isPartScan(TABLE_ID, "0")
                .soleOutput().isNull())
            .task(1, t -> t.operatorNum(1).location(MockMetaServiceProvider.LOC_1)
                .soleSource().isPartScan(TABLE_ID, "1")
                .soleOutput().isNull());
    }

    @Test
    public void testVisitExchange() {
        RelOptCluster cluster = parser.getCluster();
        DingoExchange exchange = new DingoExchange(
            cluster,
            cluster.traitSetOf(DingoConventions.PARTITIONED),
            new DingoPartScan(
                cluster,
                cluster.traitSetOf(DingoConventions.DISTRIBUTED),
                table
            )
        );
        Job job = DingoJobVisitor.createJob(exchange);
        Assert.job(job).taskNum(2)
            .task(0, t -> t.operatorNum(2).location(MockMetaServiceProvider.LOC_0).sourceNum(2)
                .source(0, s -> s.isPartScan(TABLE_ID, "0")
                    .soleOutput().isNull())
                .source(1, s -> s.isA(ReceiveOperator.class)
                    .soleOutput().isNull()))
            .task(1, t -> t.operatorNum(2).location(MockMetaServiceProvider.LOC_1)
                .soleSource().isPartScan(TABLE_ID, "1")
                .soleOutput().isA(SendOperator.class));
    }

    @Test
    public void testVisitCoalesce() {
        RelOptCluster cluster = parser.getCluster();
        DingoCoalesce coalesce = new DingoCoalesce(
            cluster,
            cluster.traitSetOf(DingoConventions.ROOT),
            new DingoExchange(
                cluster,
                cluster.traitSetOf(DingoConventions.PARTITIONED),
                new DingoPartScan(
                    cluster,
                    cluster.traitSetOf(DingoConventions.DISTRIBUTED),
                    table
                )
            )
        );
        Job job = DingoJobVisitor.createJob(coalesce);
        Assert.job(job).taskNum(2)
            .task(0, t -> t.operatorNum(3).location(MockMetaServiceProvider.LOC_0).sourceNum(2)
                .source(0, s -> s.isPartScan(TABLE_ID, "0")
                    .soleOutput().isA(CoalesceOperator.class))
                .source(1, s -> s.isA(ReceiveOperator.class)
                    .soleOutput().isA(CoalesceOperator.class)))
            .task(1, t -> t.operatorNum(2).location(MockMetaServiceProvider.LOC_1)
                .soleSource().isPartScan(TABLE_ID, "1")
                .soleOutput().isA(SendOperator.class));
    }

    @Test
    public void testVisitValues() {
        Job job = DingoJobVisitor.createJob(values);
        ValuesOperator operator = (ValuesOperator) Assert.job(job)
            .soleTask().location(MockMetaServiceProvider.LOC_0).operatorNum(1)
            .soleSource().isA(ValuesOperator.class)
            .getInstance();
        List<Object[]> tuples = operator.getTuples();
        assertThat(tuples).element(0).satisfies(obj -> {
            assertThat(obj[0]).isEqualTo(BigDecimal.valueOf(1));
            assertThat(obj[1]).isEqualTo("Alice");
            assertThat(obj[2]).isEqualTo(BigDecimal.valueOf(1));
        });
    }

    @Test
    public void testVisitPartModify() {
        RelOptCluster cluster = parser.getCluster();
        DingoPartModify partModify = new DingoPartModify(
            cluster,
            cluster.traitSetOf(DingoConventions.DISTRIBUTED),
            distributedValues,
            table,
            TableModify.Operation.INSERT,
            null,
            null
        );
        Job job = DingoJobVisitor.createJob(partModify);
        Assert.job(job).taskNum(2)
            .task(0, t -> t.location(MockMetaServiceProvider.LOC_0).operatorNum(2)
                .soleSource().isA(ValuesOperator.class)
                .soleOutput().isA(PartModifyOperator.class))
            .task(1, t -> t.location(MockMetaServiceProvider.LOC_1).operatorNum(2)
                .soleSource().isA(ValuesOperator.class)
                .soleOutput().isA(PartModifyOperator.class));
    }
}
