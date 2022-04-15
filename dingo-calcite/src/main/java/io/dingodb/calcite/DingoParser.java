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
import io.dingodb.calcite.rule.DingoRules;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.ImmutableBeans;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nonnull;

// Each sql parsing requires a new instance.
@Slf4j
public class DingoParser {
    @Getter
    private final DingoParserContext context;
    @Getter
    private final RelOptCluster cluster;
    @Getter
    private final RelOptPlanner planner;
    @Getter
    private final SqlValidator sqlValidator;
    @Getter
    private final CalciteCatalogReader catalogReader;

    protected SqlParser.Config parserConfig = SqlParser.config()
        .withLex(Lex.MYSQL)
        .withCaseSensitive(false)
        .withConformance(SqlConformanceEnum.MYSQL_5);

    protected SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
        .withSqlConformance(parserConfig.conformance());

    public DingoParser(@Nonnull DingoParserContext context) {
        this.context = context;
        planner = new VolcanoPlanner();
        // Very important, it defines the RelNode convention. Logical nodes have `Convention.NONE`.
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RexBuilder rexBuilder = new RexBuilder(context.getTypeFactory());
        cluster = RelOptCluster.create(planner, rexBuilder);

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(parserConfig.caseSensitive()));

        catalogReader = new CalciteCatalogReader(
            context.getRootSchema(),
            Collections.singletonList(context.getDefaultSchemaName()),
            context.getTypeFactory(),
            new CalciteConnectionConfigImpl(properties)
        );

        // CatalogReader is also serving as SqlOperatorTable
        sqlValidator = SqlValidatorUtil.newValidator(
            SqlOperatorTables.chain(SqlStdOperatorTable.instance(), catalogReader),
            catalogReader,
            context.getTypeFactory(),
            validatorConfig
        );
    }

    public SqlNode parse(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode = parser.parseQuery();
        if (log.isDebugEnabled()) {
            log.debug("==DINGO==>:[Input Query]: {}", sql);
            log.debug("==DINGO==>:[Parsed Query]: {}", sqlNode.toString());
        }
        return sqlNode;
    }

    public SqlNode validate(SqlNode sqlNode) {
        return sqlValidator.validate(sqlNode);
    }

    public RelDataType getValidatedNodeType(SqlNode sqlNode) {
        return sqlValidator.getValidatedNodeType(sqlNode);
    }

    public List<List<String>> getFieldOrigins(SqlNode sqlNode) {
        return sqlValidator.getFieldOrigins(sqlNode);
    }

    public RelRoot convert(@Nonnull SqlNode sqlNode) {

        SqlToRelConverter.Config convertConfig = SqlToRelConverter.config()
            .withTrimUnusedFields(true)
            .withExpand(false)
            .withExplain(sqlNode.getKind() == SqlKind.EXPLAIN);

        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
            (PlannerImpl) Frameworks.getPlanner(Frameworks.newConfigBuilder().build()),
            sqlValidator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            convertConfig
        );

        RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, false, true);
        if (log.isDebugEnabled()) {
            String relRootString = RelOptUtil.dumpPlan(
                "[Physical plan before optimization]",
                relRoot.rel,
                SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
            log.debug("==DINGO==>:[SqlNode Converted RelRoot] {}", relRootString);
        }
        return relRoot;
    }

    public RelNode optimize(RelNode relNode) {
        return optimize(relNode, DingoConventions.ROOT);
    }

    public RelNode optimize(RelNode relNode, Convention convention) {
        RelTraitSet traitSet = planner.emptyTraitSet().replace(convention);
        List<RelOptRule> rules = DingoRules.rules();
        if (convention == EnumerableConvention.INSTANCE) {
            rules = ImmutableList.<RelOptRule>builder()
                .addAll(rules)
                .add(DingoRules.DINGO_TO_ENUMERABLE_RULE)
                .build();
        }
        final Program program = Programs.ofRules(rules);
        RelNode optimizedRelNode = program.run(planner, relNode, traitSet, ImmutableList.of(), ImmutableList.of());
        if (log.isDebugEnabled()) {
            String relNodeString = RelOptUtil.dumpPlan(
                "[Physical plan after optimization]",
                optimizedRelNode,
                SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
            log.debug("==DINGO==>:[Optimized RelNode] {}", relNodeString);
        }
        return optimizedRelNode;
    }
}
