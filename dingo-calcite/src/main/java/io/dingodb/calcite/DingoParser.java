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
import io.dingodb.calcite.meta.DingoRelMetadataProvider;
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rule.DingoRules;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nonnull;

// Each sql parsing requires a new instance.
@Slf4j
public class DingoParser {
    public static SqlParser.Config PARSER_CONFIG = SqlParser.config()
        .withLex(Lex.MYSQL)
        .withCaseSensitive(false)
        .withConformance(new SqlDelegatingConformance(SqlConformanceEnum.MYSQL_5) {
            // Allows some system functions with no parameters to be used with Parentheses.
            // for example, `CURRENT_DATE`.
            @Override
            public boolean allowNiladicParentheses() {
                return true;
            }
        });
    public static SqlValidator.Config VALIDATOR_CONFIG = SqlValidator.Config.DEFAULT
        .withConformance(PARSER_CONFIG.conformance());
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

    public DingoParser(@Nonnull DingoParserContext context) {
        this.context = context;
        planner = new VolcanoPlanner();
        // Very important, it defines the RelNode convention. Logical nodes have `Convention.NONE`.
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        // Defines the "order-by" traits.
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        RexBuilder rexBuilder = new RexBuilder(context.getTypeFactory());
        cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(ChainedRelMetadataProvider.of(
            ImmutableList.of(
                DingoRelMetadataProvider.INSTANCE,
                Objects.requireNonNull(cluster.getMetadataProvider())
            )
        ));

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(PARSER_CONFIG.caseSensitive()));

        catalogReader = new CalciteCatalogReader(
            context.getRootSchema(),
            Collections.singletonList(context.getDefaultSchemaName()),
            context.getTypeFactory(),
            new CalciteConnectionConfigImpl(properties)
        );

        // CatalogReader is also serving as SqlOperatorTable.
        SqlStdOperatorTable tableInstance = SqlStdOperatorTable.instance();
        sqlValidator = SqlValidatorUtil.newValidator(
            SqlOperatorTables.chain(tableInstance, catalogReader),
            catalogReader,
            context.getTypeFactory(),
            VALIDATOR_CONFIG
        );
    }

    @SuppressWarnings("MethodMayBeStatic")
    public SqlNode parse(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
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

    public RelDataType getParameterRowType(SqlNode sqlNode) {
        return sqlValidator.getParameterRowType(sqlNode);
    }

    public RelRoot convert(@Nonnull SqlNode sqlNode) {
        return convert(sqlNode, true);
    }

    public RelRoot convert(@Nonnull SqlNode sqlNode, boolean needsValidation) {
        SqlToRelConverter.Config convertConfig = SqlToRelConverter.config()
            .withTrimUnusedFields(true)
            .withExpand(false)
            .withExplain(sqlNode.getKind() == SqlKind.EXPLAIN)
            // Disable simplify to use Dingo's own expr evaluation.
            .addRelBuilderConfigTransform(c -> c.withSimplify(false));

        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
            ViewExpanders.simpleContext(cluster),
            sqlValidator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            convertConfig
        );

        RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, needsValidation, true);

        // Insert a `DingoRoot` to collect the results.
        return relRoot.withRel(new DingoRoot(cluster, planner.emptyTraitSet(), relRoot.rel));
    }

    public RelNode optimize(RelNode relNode) {
        RelTraitSet traitSet = planner.emptyTraitSet().replace(DingoConventions.ROOT);
        List<RelOptRule> rules = DingoRules.rules();
        final Program program = Programs.ofRules(rules);
        return program.run(planner, relNode, traitSet, ImmutableList.of(), ImmutableList.of());
    }
}
