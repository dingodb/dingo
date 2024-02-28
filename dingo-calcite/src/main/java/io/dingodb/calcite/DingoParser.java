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
import io.dingodb.calcite.grammar.ddl.SqlAnalyze;
import io.dingodb.calcite.grammar.ddl.SqlBeginTx;
import io.dingodb.calcite.grammar.ddl.SqlCall;
import io.dingodb.calcite.grammar.ddl.SqlCommit;
import io.dingodb.calcite.grammar.ddl.SqlKillConnection;
import io.dingodb.calcite.grammar.ddl.SqlKillQuery;
import io.dingodb.calcite.grammar.ddl.SqlLoadData;
import io.dingodb.calcite.grammar.ddl.SqlLockBlock;
import io.dingodb.calcite.grammar.ddl.SqlLockTable;
import io.dingodb.calcite.grammar.ddl.SqlRollback;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import io.dingodb.calcite.grammar.ddl.SqlUnLockBlock;
import io.dingodb.calcite.grammar.ddl.SqlUnLockTable;
import io.dingodb.calcite.grammar.dml.SqlExecute;
import io.dingodb.calcite.grammar.dml.SqlPrepare;
import io.dingodb.calcite.grammar.dql.ExportOptions;
import io.dingodb.calcite.grammar.dql.SqlDesc;
import io.dingodb.calcite.grammar.dql.SqlNextAutoIncrement;
import io.dingodb.calcite.grammar.dql.SqlShow;
import io.dingodb.calcite.meta.DingoRelMetadataProvider;
import io.dingodb.calcite.operation.Operation;
import io.dingodb.calcite.operation.SqlToOperationConverter;
import io.dingodb.calcite.rel.DingoCost;
import io.dingodb.calcite.rel.logical.LogicalDingoRoot;
import io.dingodb.calcite.rel.LogicalExportData;
import io.dingodb.calcite.rule.DingoRules;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.traits.DingoRelStreamingDef;
import io.dingodb.common.error.DingoError;
import io.dingodb.common.error.DingoException;
import io.dingodb.common.type.TupleMapping;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringEscapeUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.File;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rule.DingoRules.DINGO_AGGREGATE_REDUCE_RULE;
import static io.dingodb.calcite.rule.DingoRules.DINGO_AGGREGATE_RULE;
import static io.dingodb.calcite.rule.DingoRules.DINGO_AGGREGATE_SCAN_RULE;
import static io.dingodb.calcite.rule.DingoRules.DINGO_FILTER_RULE;
import static io.dingodb.calcite.rule.DingoRules.DINGO_PROJECT_RULE;
import static io.dingodb.calcite.rule.dingo.DingoPhysicalRules.DINGO_REDUCE_AGGREGATE_RULE;
import static io.dingodb.calcite.rule.dingo.DingoPhysicalRules.DINGO_REL_OP_RULE;
import static io.dingodb.calcite.rule.dingo.DingoPhysicalRules.DINGO_SCAN_WITH_REL_OP_RULE;
import static io.dingodb.calcite.rule.logical.DingoLogicalRules.LOGICAL_MERGE_REL_OP_SCAN_RULE;
import static io.dingodb.calcite.rule.logical.DingoLogicalRules.LOGICAL_REL_OP_FROM_FILTER_RULE;
import static io.dingodb.calcite.rule.logical.DingoLogicalRules.LOGICAL_REL_OP_FROM_PROJECT_RULE;
import static io.dingodb.calcite.rule.logical.DingoLogicalRules.LOGICAL_SCAN_WITH_REL_OP_RULE;
import static io.dingodb.calcite.rule.logical.DingoLogicalRules.LOGICAL_SPLIT_AGGREGATE_RULE;

// Each sql parsing requires a new instance.
@Slf4j
public class DingoParser {
    private static final Map<String, String> sensitiveKey = new HashMap<>();

    static {
        sensitiveKey.put(".\"USER\"", ".USER");
    }

    public static SqlParser.Config PARSER_CONFIG = SqlParser.config()
        .withLex(Lex.MYSQL)
        .withCaseSensitive(false)
        .withIdentifierMaxLength(100000)
        .withParserFactory(DingoDdlParserFactory.INSTANCE)
        .withConformance(new SqlDelegatingConformance(SqlConformanceEnum.MYSQL_5) {
            // Allows some system functions with no parameters to be used with Parentheses.
            // for example, `CURRENT_DATE`.
            @Override
            public boolean allowNiladicParentheses() {
                return true;
            }

            @Override
            public boolean isLimitStartCountAllowed() {
                return true;
            }

            @Override
            public boolean isOffsetLimitAllowed() {
                return true;
            }

            /**
             * Whether to allow INSERT (or UPSERT) with no column list but fewer values than the target table.
             * If a table does not have a primary key but has a hidden primary key _rowid,
             * then it is necessary to support insert into table values ('value ')
             * @return true
             */
            @Override
            public boolean isInsertSubsetColumnsAllowed() {
                return false;
            }
        });

    @Getter
    private final DingoParserContext context;
    @Getter
    private final RelOptCluster cluster;
    @Getter
    private final VolcanoPlanner planner;
    @Getter
    private final DingoSqlValidator sqlValidator;

    public DingoParser(final @NonNull DingoParserContext context) {
        this.context = context;

        // Create Planner.
        planner = new VolcanoPlanner(DingoCost.FACTORY, context);
        // Set to `true` to use `TopDownRuleDriver`, or `IterativeRuleDriver` is used.
        // It seems that `TopDownRuleDriver` is faster than `IterativeRuleDriver`.
        planner.setTopDownOpt(context.getConfig().topDownOpt());
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

        // Create SqlValidator
        sqlValidator = context.getSqlValidator();

        context.resetSchemaCache();
    }

    @SuppressWarnings("MethodMayBeStatic")
    public SqlNode parse(String sql) throws SqlParseException {
        sql = processKeyWords(sql);
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        SqlNode sqlNode = parser.parseQuery();
        if (log.isDebugEnabled()) {
            log.debug("==DINGO==>:[Input Query]: {}", sql);
            log.debug("==DINGO==>:[Parsed Query]: {}", sqlNode.toString());
        }
        return sqlNode;
    }

    public RelRoot convert(@NonNull SqlNode sqlNode) {
        return convert(sqlNode, true);
    }

    public RelRoot convert(@NonNull SqlNode sqlNode, boolean needsValidation) {
        HintPredicate hintPredicate = new HintPredicate() {
            @Override
            public boolean apply(RelHint hint, RelNode rel) {
                return true;
            }
        };
        HintStrategyTable hintStrategyTable = new HintStrategyTable.Builder()
            .hintStrategy("vector_pre", hintPredicate).build();
        SqlToRelConverter sqlToRelConverter = new DingoSqlToRelConverter(
            ViewExpanders.simpleContext(cluster),
            sqlValidator,
            context.getCatalogReader(),
            cluster,
            sqlNode.getKind() == SqlKind.EXPLAIN,
            hintStrategyTable
        );

        RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, needsValidation, true);

        RelNode relNode = relRoot.rel;
        TupleMapping selection = null;
        if (relRoot.kind == SqlKind.SELECT) {
            selection = TupleMapping.of(
                relRoot.fields.stream().map(Pair::getKey).collect(Collectors.toList())
            );

            if (needExport(sqlNode)) {
                io.dingodb.calcite.grammar.dql.SqlSelect sqlSelect = (io.dingodb.calcite.grammar.dql.SqlSelect) sqlNode;
                validatorExportParam(sqlSelect.getExportOptions());
                relNode = new LogicalExportData(
                    cluster,
                    planner.emptyTraitSet(),
                    relRoot.rel,
                    sqlSelect.getOutfile(),
                    sqlSelect.getTerminated(),
                    sqlSelect.getSqlId(),
                    sqlSelect.getEnclosed(),
                    sqlSelect.getLineTerminated(),
                    sqlSelect.getEscaped(),
                    sqlSelect.getCharset(),
                    sqlSelect.getLineStarting(),
                    context.getTimeZone()
                );
            }
        }
        // Insert a `DingoRoot` to collect the results.
        return relRoot.withRel(new LogicalDingoRoot(cluster, planner.emptyTraitSet(), relNode, selection));
    }

    private static boolean needExport(@NonNull SqlNode sqlNode) {
        if (sqlNode instanceof io.dingodb.calcite.grammar.dql.SqlSelect) {
            io.dingodb.calcite.grammar.dql.SqlSelect sqlSelect = (io.dingodb.calcite.grammar.dql.SqlSelect) sqlNode;
            return sqlSelect.isExport();
        }
        return false;
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
        ImmutableList.Builder<RelOptRule> builder = ImmutableList.builder();
        builder.addAll(rules);
        if (!context.getConfig().topDownOpt()) {
            // This is needed for `IterativeRuleDriver`.
            builder.add(AbstractConverter.ExpandConversionRule.INSTANCE);
        }
        if (context.isUsingRelOp()) {
            builder.add(LOGICAL_SCAN_WITH_REL_OP_RULE);
            builder.add(LOGICAL_REL_OP_FROM_FILTER_RULE);
            builder.add(LOGICAL_REL_OP_FROM_PROJECT_RULE);
            builder.add(LOGICAL_SPLIT_AGGREGATE_RULE);
            builder.add(LOGICAL_MERGE_REL_OP_SCAN_RULE);
            builder.add(DINGO_REL_OP_RULE);
            builder.add(DINGO_SCAN_WITH_REL_OP_RULE);
            builder.add(DINGO_REDUCE_AGGREGATE_RULE);
        } else {
            builder.add(DINGO_FILTER_RULE);
            builder.add(DINGO_PROJECT_RULE);
            builder.add(DINGO_AGGREGATE_RULE);
            builder.add(DINGO_AGGREGATE_REDUCE_RULE);
            if (context.isPushDown()) {
                builder.add(DINGO_AGGREGATE_SCAN_RULE);
            }
        }
        final Program program = Programs.ofRules(builder.build());
        // Seems the only way to prevent rex simplifying in optimization.
        try (Hook.Closeable ignored = Hook.REL_BUILDER_SIMPLIFY.addThread((Holder<Boolean> h) -> h.set(false))) {
            return program.run(planner, relNode, traitSet, ImmutableList.of(), ImmutableList.of());
        }
    }

    protected static boolean compatibleMysql(SqlNode sqlNode) {
        if (sqlNode instanceof SqlShow || sqlNode instanceof SqlDesc || sqlNode instanceof SqlNextAutoIncrement) {
            return true;
        } else if (sqlNode instanceof SqlSelect) {
            return compatibleSelect((SqlSelect) sqlNode);
        } else if (sqlNode instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
            if (sqlOrderBy.query instanceof io.dingodb.calcite.grammar.dql.SqlSelect) {
                return compatibleSelect((SqlSelect) sqlOrderBy.query);
            }
            return false;
        } else if (sqlNode instanceof SqlSetOption && !(sqlNode instanceof SqlSetPassword)) {
            return true;
        } else if (sqlNode instanceof SqlPrepare
            || sqlNode instanceof SqlExecute
            || sqlNode instanceof SqlAnalyze
            || sqlNode instanceof SqlBeginTx
            || sqlNode instanceof SqlCommit
            || sqlNode instanceof SqlRollback
            || sqlNode instanceof SqlLockTable
            || sqlNode instanceof SqlLockBlock
            || sqlNode instanceof SqlUnLockTable
            || sqlNode instanceof SqlUnLockBlock
            || sqlNode instanceof SqlKillQuery
            || sqlNode instanceof SqlKillConnection
            || sqlNode instanceof SqlLoadData
            || sqlNode instanceof SqlCall
        ) {
            return true;
        }
        return false;
    }

    private static boolean compatibleSelect(SqlSelect sqlNode) {
        SqlNodeList sqlNodes = sqlNode.getSelectList();
        return sqlNodes.stream().allMatch(e -> {
            if (e instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) e;
                String operatorName = sqlBasicCall.getOperator().getName();
                if (operatorName.equalsIgnoreCase("AS")) {
                    SqlNode sqlNode1 = sqlBasicCall.getOperandList().get(0);
                    if (sqlNode1 instanceof SqlBasicCall) {
                        operatorName = ((SqlBasicCall) sqlNode1).getOperator().getName();
                        return operatorName.equalsIgnoreCase("database")
                            || operatorName.equals("@")
                            || operatorName.equals("@@");
                    }
                } else {
                    return operatorName.equalsIgnoreCase("database")
                        || operatorName.equals("@")
                        || operatorName.equals("@@");
                }
            }
            return false;
        });
    }

    public static Operation convertToOperation(SqlNode sqlNode, Connection connection, DingoParserContext context) {
        return SqlToOperationConverter.convert(sqlNode, connection, context)
            .orElseThrow(() -> DingoException.from(DingoError.UNKNOWN));
    }

    private String processKeyWords(String sql) {
        if (sql.contains("\\r\\n") || sql.contains("\\n")) {
            sql = StringEscapeUtils.unescapeJson(sql);
        }
        for (Map.Entry<String, String> entry : sensitiveKey.entrySet()) {
            if (sql.contains(entry.getKey())) {
                sql = sql.replace(entry.getKey(), entry.getValue());
            }
        }
        return sql;
    }

    public static void validatorExportParam(ExportOptions exportOptions) {
        File file = new File(exportOptions.getOutfile());
        if (file.exists()) {
            throw DingoResource.DINGO_RESOURCE.exportFileExists(exportOptions.getOutfile()).ex();
        }
        String enclosed = exportOptions.getEnclosed();
        if (enclosed != null && enclosed.equals("()")) {
            throw DingoResource.DINGO_RESOURCE.fieldSeparatorError().ex();
        }
    }

}
