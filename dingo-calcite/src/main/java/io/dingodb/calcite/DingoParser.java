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
import io.dingodb.calcite.grammar.SqlUserDefinedOperators;
import io.dingodb.calcite.meta.DingoRelMetadataProvider;
import io.dingodb.calcite.rel.LogicalDingoRoot;
import io.dingodb.calcite.rule.DingoRules;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.traits.DingoRelStreamingDef;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Hook;
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
import org.apache.calcite.sql2rel.SqlLikeBinaryOperator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

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
    private final CalciteConnectionConfig config;
    @Getter
    private final RelOptCluster cluster;
    @Getter
    private final VolcanoPlanner planner;
    @Getter
    private final SqlValidator sqlValidator;
    @Getter
    private final CalciteCatalogReader catalogReader;

    public DingoParser(@NonNull DingoParserContext context) {
        this(context, null);
    }

    public DingoParser(
        final @NonNull DingoParserContext context,
        final @Nullable CalciteConnectionConfig config
    ) {
        this.context = context;
        CalciteConnectionConfigImpl newConfig;
        if (config != null) {
            newConfig = (CalciteConnectionConfigImpl) config;
        } else {
            newConfig = new CalciteConnectionConfigImpl(new Properties());
        }
        newConfig = newConfig
            .set(CalciteConnectionProperty.CASE_SENSITIVE, String.valueOf(PARSER_CONFIG.caseSensitive()));
        //.set(CalciteConnectionProperty.TOPDOWN_OPT, String.valueOf(true));
        this.config = newConfig;

        // Create Planner.
        planner = new VolcanoPlanner(context);
        // Set to `true` to use `TopDownRuleDriver`, or `IterativeRuleDriver` is used.
        // It seems that `TopDownRuleDriver` is faster than `IterativeRuleDriver`.
        planner.setTopDownOpt(this.config.topDownOpt());
        // Very important, it defines the RelNode convention. Logical nodes have `Convention.NONE`.
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(DingoRelStreamingDef.INSTANCE);
        // Defines the "order-by" traits.
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        // Create Cluster.
        RexBuilder rexBuilder = new RexBuilder(context.getTypeFactory());
        cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(ChainedRelMetadataProvider.of(
            ImmutableList.of(
                DingoRelMetadataProvider.INSTANCE,
                Objects.requireNonNull(cluster.getMetadataProvider())
            )
        ));

        // Create CatalogReader.
        catalogReader = new CalciteCatalogReader(
            context.getRootSchema(),
            Collections.singletonList(context.getDefaultSchemaName()),
            context.getTypeFactory(),
            this.config
        );

        // Create SqlValidator.
        // CatalogReader is also serving as SqlOperatorTable.
        SqlStdOperatorTable tableInstance = SqlStdOperatorTable.instance();
        // Register operators
        tableInstance.register(SqlUserDefinedOperators.LIKE_BINARY);
        tableInstance.register(SqlUserDefinedOperators.NOT_LIKE_BINARY);
        SqlLikeBinaryOperator.register();

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

    public RelRoot convert(@NonNull SqlNode sqlNode) {
        return convert(sqlNode, true);
    }

    public RelRoot convert(@NonNull SqlNode sqlNode, boolean needsValidation) {
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
        return relRoot.withRel(new LogicalDingoRoot(cluster, planner.emptyTraitSet(), relRoot.rel));
    }

    /**
     * Optimize a {@link RelNode} tree.
     *
     * @param relNode the input {@link RelNode}
     * @return the optimized {@link RelNode}
     */
    public RelNode optimize(RelNode relNode) {
        RelTraitSet traitSet = planner.emptyTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.ROOT);
        List<RelOptRule> rules = DingoRules.rules();
        if (!config.topDownOpt()) {
            rules = ImmutableList.<RelOptRule>builder()
                .addAll(rules)
                // This is needed for `IterativeRuleDriver`.
                .add(AbstractConverter.ExpandConversionRule.INSTANCE)
                .build();
        }
        final Program program = Programs.ofRules(rules);
        // Seems the only way to prevent rex simplifying in optimization.
        try (Hook.Closeable ignored = Hook.REL_BUILDER_SIMPLIFY.addThread((Holder<Boolean> h) -> h.set(false))) {
            return program.run(planner, relNode, traitSet, ImmutableList.of(), ImmutableList.of());
        }
    }
}
