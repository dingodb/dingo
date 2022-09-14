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

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.assertion.Assert;
import io.dingodb.calcite.mock.MockMetaServiceProvider;
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rule.DingoRules;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

public class TestRelBuilder {
    @Test
    public void testFilterProjectScan() {
        DingoParserContext context = new DingoParserContext(MockMetaServiceProvider.SCHEMA_NAME);
        DingoParser parser = new DingoParser(context);
        CalciteSchema schema = parser.getContext().getDefaultSchema();
        FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(DingoParser.PARSER_CONFIG)
            .sqlValidatorConfig(DingoParser.VALIDATOR_CONFIG)
            .defaultSchema(schema.plus())
            .build();
        final RelBuilder builder = RelBuilder.create(config);
        final RelOptCluster cluster = builder.getCluster();
        final RelNode relNode = builder.scan("test")
            .project(builder.field("ID"), builder.field("NAME"))
            .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field("ID"), builder.literal(1)))
            .push(new DingoRoot(cluster, cluster.traitSet(), builder.build()))
            .build();
        final Program program = Programs.ofRules(DingoRules.rules());
        // `RelBuilder` has its own planner embedded, so cannot use other planner to do optimizing.
        RelOptPlanner planner = relNode.getCluster().getPlanner();
        RelTraitSet traitSet = planner.emptyTraitSet().replace(DingoConventions.ROOT);
        RelNode optimized = program.run(planner, relNode, traitSet, ImmutableList.of(), ImmutableList.of());
        Assert.relNode(optimized).isA(DingoRoot.class)
            .singleInput().isA(DingoCoalesce.class)
            .singleInput().isA(DingoExchange.class)
            .singleInput().isA(DingoGetByKeys.class);
    }
}
