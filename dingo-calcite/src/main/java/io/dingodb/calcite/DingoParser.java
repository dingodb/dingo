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

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ibm.icu.impl.locale.XCldrStub;
import io.dingodb.calcite.executor.Executor;
import io.dingodb.calcite.executor.SqlToExecutorConverter;
import io.dingodb.calcite.grammar.ddl.DingoSqlCreateTable;
import io.dingodb.calcite.grammar.ddl.SqlAdminResetAutoInc;
import io.dingodb.calcite.grammar.ddl.SqlAdminRollback;
import io.dingodb.calcite.grammar.ddl.SqlAlterAddColumn;
import io.dingodb.calcite.grammar.ddl.SqlAlterAddIndex;
import io.dingodb.calcite.grammar.ddl.SqlAlterAutoIncrement;
import io.dingodb.calcite.grammar.ddl.SqlAlterChangeColumn;
import io.dingodb.calcite.grammar.ddl.SqlAlterColumn;
import io.dingodb.calcite.grammar.ddl.SqlAlterDropColumn;
import io.dingodb.calcite.grammar.ddl.SqlAlterDropIndex;
import io.dingodb.calcite.grammar.ddl.SqlAlterDropPart;
import io.dingodb.calcite.grammar.ddl.SqlAlterExchangePart;
import io.dingodb.calcite.grammar.ddl.SqlAlterIndex;
import io.dingodb.calcite.grammar.ddl.SqlAlterModifyColumn;
import io.dingodb.calcite.grammar.ddl.SqlAlterRenameIndex;
import io.dingodb.calcite.grammar.ddl.SqlAlterRenameTable;
import io.dingodb.calcite.grammar.ddl.SqlAlterTableAddPart;
import io.dingodb.calcite.grammar.ddl.SqlAlterTableComment;
import io.dingodb.calcite.grammar.ddl.SqlAlterTableOptions;
import io.dingodb.calcite.grammar.ddl.SqlAlterTruncatePart;
import io.dingodb.calcite.grammar.ddl.SqlAnalyze;
import io.dingodb.calcite.grammar.ddl.SqlBeginTx;
import io.dingodb.calcite.grammar.ddl.SqlCall;
import io.dingodb.calcite.grammar.ddl.SqlCommit;
import io.dingodb.calcite.grammar.ddl.SqlCreateSchema;
import io.dingodb.calcite.grammar.ddl.SqlCreateSequence;
import io.dingodb.calcite.grammar.ddl.SqlCreateUser;
import io.dingodb.calcite.grammar.ddl.SqlDropSequence;
import io.dingodb.calcite.grammar.ddl.SqlDropUser;
import io.dingodb.calcite.grammar.ddl.SqlFlashBackSchema;
import io.dingodb.calcite.grammar.ddl.SqlFlashBackTable;
import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.ddl.SqlInitSchema;
import io.dingodb.calcite.grammar.ddl.SqlKillConnection;
import io.dingodb.calcite.grammar.ddl.SqlKillQuery;
import io.dingodb.calcite.grammar.ddl.SqlLoadData;
import io.dingodb.calcite.grammar.ddl.SqlLockBlock;
import io.dingodb.calcite.grammar.ddl.SqlLockTable;
import io.dingodb.calcite.grammar.ddl.SqlRecoverTable;
import io.dingodb.calcite.grammar.ddl.SqlRollback;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import io.dingodb.calcite.grammar.ddl.SqlTruncate;
import io.dingodb.calcite.grammar.ddl.SqlUnLockBlock;
import io.dingodb.calcite.grammar.ddl.SqlUnLockTable;
import io.dingodb.calcite.grammar.dml.SqlExecute;
import io.dingodb.calcite.grammar.dml.SqlInsert;
import io.dingodb.calcite.grammar.dml.SqlPrepare;
import io.dingodb.calcite.grammar.dml.SqlUpdate;
import io.dingodb.calcite.grammar.dql.ExportOptions;
import io.dingodb.calcite.grammar.dql.FlashBackSqlIdentifier;
import io.dingodb.calcite.grammar.dql.SqlBackUpTimePoint;
import io.dingodb.calcite.grammar.dql.SqlBackUpTsoPoint;
import io.dingodb.calcite.grammar.dql.SqlNextAutoIncrement;
import io.dingodb.calcite.grammar.dql.SqlSelect;
import io.dingodb.calcite.grammar.dql.SqlShow;
import io.dingodb.calcite.grammar.dql.SqlStartGc;
import io.dingodb.calcite.grammar.dql.SqlTenantsBackUpTimePoint;
import io.dingodb.calcite.grammar.dql.SqlTsoToTime;
import io.dingodb.calcite.meta.DingoRelMetadataProvider;
import io.dingodb.calcite.program.DecorrelateProgram;
import io.dingodb.calcite.rel.DingoCost;
import io.dingodb.calcite.rel.LogicalExportData;
import io.dingodb.calcite.rel.logical.LogicalDingoRoot;
import io.dingodb.calcite.rule.DingoRules;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.calcite.traits.DingoRelStreamingDef;
import io.dingodb.calcite.utils.SqlUtil;
import io.dingodb.common.error.DingoError;
import io.dingodb.common.error.DingoException;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.log.SqlLogUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.mysql.DingoErrUtil;
import io.dingodb.common.profile.PlanProfile;
import io.dingodb.common.table.HybridSearchTable;
import io.dingodb.common.time.DingoTimeZoneContext;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.InfoSchemaService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
import static io.dingodb.common.mysql.error.ErrorCode.ErrSetDiffTime;
import static io.dingodb.common.util.NameCaseUtils.caseSensitive;

// Each sql parsing requires a new instance.
@Slf4j
public class DingoParser {
    private static final Map<String, String> sensitiveKey = new HashMap<>();

    static {
        sensitiveKey.put(".\"USER\"", ".USER");
        // for mysql dump start
        sensitiveKey.put("GROUP BY LOGFILE_GROUP_NAME, FILE_NAME, ENGINE, TOTAL_EXTENTS, INITIAL_SIZE "
            + "ORDER BY LOGFILE_GROUP_NAME", "GROUP BY LOGFILE_GROUP_NAME, FILE_NAME, ENGINE, TOTAL_EXTENTS, "
            + "INITIAL_SIZE, EXTRA ORDER BY LOGFILE_GROUP_NAME");
        // for mysql dump end
    }

    public static SqlParser.Config PARSER_CONFIG = SqlParser.config()
        .withLex(Lex.MYSQL)
        .withCaseSensitive(caseSensitive())
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
             * If a table does not have a primary key but has a hidden primary key IMPLICIT_COL_NAME,
             * then it is necessary to support insert into table values ('value ')
             * @return true
             */
            @Override
            public boolean isInsertSubsetColumnsAllowed() {
                return false;
            }

            @Override
            public boolean allowCharLiteralAlias() {
                return true;
            }

            @Override
            public boolean isPercentRemainderAllowed() {
                return true;
            }

        });

    protected static Set<Class> ddlResultSet = ImmutableSet.of(
        SqlAlterAutoIncrement.class,
        DingoSqlCreateTable.class
    );

    @Getter
    private final DingoParserContext context;
    @Getter
    private final RelOptCluster cluster;
    @Getter
    private final VolcanoPlanner planner;
    @Getter
    private final DingoSqlValidator sqlValidator;

    protected long pointTs;

    public DingoParser(final @NonNull DingoParserContext context) {
        this.context = context;
        // Create Planner.
        planner = new VolcanoPlanner(DingoCost.FACTORY, context);
        //planner.setExecutor(new RexExecutorImpl(DataContexts.EMPTY));
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

        //context.resetSchemaCache();
    }

    public SqlNode parse(String sql) throws SqlParseException {
        sql = processKeyWords(sql);
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        SqlNode sqlNode = parser.parseQuery();
        if (sqlNode instanceof SqlAlterTableOptions) {
            ((SqlAlterTableOptions) sqlNode).setSql(sql);
        }
        if (StringUtils.isEmpty(context.getOption("sql_log"))) {
            SqlLogUtils.info("Input Query: {}", SqlUtil.checkSql(sqlNode, sql));
        }
        LogUtils.trace(log, "==DINGO==>:[Parsed Query]: {}", sqlNode.toString());
        return sqlNode;
    }

    public RelRoot convert(@NonNull SqlNode sqlNode) {
        return convert(sqlNode, true);
    }

    public RelRoot convert(@NonNull SqlNode sqlNode, boolean needsValidation) {
        HintPredicate hintPredicate = (hint, rel) -> true;
        HintStrategyTable.Builder hintStrategyBuilder = new HintStrategyTable.Builder()
            .hintStrategy("vector_pre", hintPredicate)
            .hintStrategy(HybridSearchTable.HINT_NAME, hintPredicate)
            .hintStrategy("disable_index", hintPredicate)
            .hintStrategy("text_search_pre", hintPredicate);
        if (forUpdate(sqlNode)) {
            hintStrategyBuilder.hintStrategy("for_update", hintPredicate);
        }
        SqlToRelConverter sqlToRelConverter = new DingoSqlToRelConverter(
            ViewExpanders.simpleContext(cluster),
            sqlValidator,
            context.getCatalogReader(),
            cluster,
            sqlNode.getKind() == SqlKind.EXPLAIN,
            hintStrategyBuilder.build()
        );

        RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, needsValidation, true);

        RelNode relNode = relRoot.rel;
        TupleMapping selection = null;
        if (relRoot.kind == SqlKind.SELECT) {
            selection = TupleMapping.of(
                relRoot.fields.stream().map(Pair::getKey).collect(Collectors.toList())
            );

            if (needExport(sqlNode)) {
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
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
                    DingoTimeZoneContext.getTimeZone()
                );
            }
        }
        // Insert a `DingoRoot` to collect the results.
        return relRoot.withRel(new LogicalDingoRoot(cluster, planner.emptyTraitSet(), relNode, selection));
    }

    public static boolean needExport(@NonNull SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            return sqlSelect.isExport();
        }
        return false;
    }

    public static boolean flashBackQuery(@NonNull SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            if (sqlSelect.getFrom() instanceof FlashBackSqlIdentifier) {
                return true;
            } else {
                return sqlSelect.isFlashBackQuery();
            }
        } else if (sqlNode instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
            if (sqlOrderBy.query instanceof SqlSelect) {
                SqlSelect sqlSelect = (SqlSelect) sqlOrderBy.query;
                if (sqlSelect.getFrom() instanceof FlashBackSqlIdentifier) {
                    return true;
                } else {
                    return sqlSelect.isFlashBackQuery();
                }
            }
        } else if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            sqlBasicCall.getOperandList().forEach(sqlNode1 -> {
                if (sqlNode1 instanceof SqlSelect) {
                    SqlSelect sqlSelect = (SqlSelect) sqlNode1;
                    if (sqlSelect.getFrom() instanceof FlashBackSqlIdentifier) {
                        throw DingoErrUtil.newStdErr(ErrSetDiffTime);
                    } else if (sqlSelect.isFlashBackQuery()) {
                        throw DingoErrUtil.newStdErr(ErrSetDiffTime);
                    }
                }
            });
        }
        return false;
    }

    public static boolean forUpdate(@NonNull SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            return sqlSelect.isForUpdate();
        }
        return false;
    }

    public static boolean getReplaceInto(@NonNull SqlNode sqlNode) {
        if (sqlNode instanceof SqlInsert) {
            SqlInsert sqlInsert = (SqlInsert) sqlNode;
            return sqlInsert.isReplace();
        }
        return false;
    }

    public static boolean getIgnore(@NonNull SqlNode sqlNode) {
        if (sqlNode instanceof SqlInsert) {
            SqlInsert sqlInsert = (SqlInsert) sqlNode;
            return sqlInsert.isIgnore();
        }
        return false;
    }

    public static long getUpdateLimit(@NonNull SqlNode sqlNode) {
        if (sqlNode instanceof SqlUpdate) {
            SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;
            return sqlUpdate.getLimit();
        }
        return -1L;
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

        builder.addAll(DingoRules.ABSTRACT_RELATIONAL_RULES);
        builder.addAll(DingoRules.ABSTRACT_RULES);
        builder.addAll(DingoRules.BASE_RULES);

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
        builder.add(DingoRules.FILTER_REDUCE_EXPRESSIONS_RULE);
        final Program program = Programs.ofRules(builder.build());
        // Seems the only way to prevent rex simplifying in optimization.
        try (Hook.Closeable ignored = Hook.REL_BUILDER_SIMPLIFY.addThread((Holder<Boolean> h) -> h.set(false))) {
            Timer.Context timeCtx = DingoMetrics.getTimeContext("deCorrelateProgram");

            Program subQueryProgram = subQuery(cluster.getMetadataProvider());
            RelNode relNode1 = subQueryProgram.run(planner, relNode, traitSet, ImmutableList.of(), ImmutableList.of());

            DecorrelateProgram decorrelateProgram = new DecorrelateProgram();
            RelNode relNode2 = decorrelateProgram.run(
                planner, relNode1, traitSet, ImmutableList.of(), ImmutableList.of()
            );
            timeCtx.stop();
            return program.run(planner, relNode2, traitSet, ImmutableList.of(), ImmutableList.of());
        }
    }

    public static Program subQuery(RelMetadataProvider metadataProvider) {
        HepProgramBuilder builder = HepProgram.builder();
        builder.addRuleCollection(ImmutableList.of(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE, CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE, CoreRules.JOIN_SUB_QUERY_TO_CORRELATE, DingoRules.UNION_ALL_ADD_PROJECT_RULE));
        return Programs.of(builder.build(), true, metadataProvider);
    }

    protected static boolean compatibleMysql(SqlNode sqlNode, PlanProfile planProfile) {
        if (sqlNode instanceof SqlShow || sqlNode instanceof SqlNextAutoIncrement) {
            planProfile.setStmtType("show");
            return true;
        } else if (sqlNode instanceof SqlSetOption && !(sqlNode instanceof SqlSetPassword)) {
            planProfile.setStmtType("set");
            return true;
        } else {
            return sqlNode instanceof SqlPrepare
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
                || sqlNode instanceof SqlAdminRollback
                || sqlNode instanceof SqlStartGc
                || sqlNode instanceof SqlBackUpTimePoint
                || sqlNode instanceof SqlBackUpTsoPoint
                || sqlNode instanceof SqlTsoToTime
                || sqlNode instanceof SqlTenantsBackUpTimePoint
                || sqlNode instanceof SqlInitSchema;
        }
    }

    public static Executor convertToOperation(SqlNode sqlNode, Connection connection, DingoParserContext context) {
        return SqlToExecutorConverter.convert(sqlNode, connection, context)
            .orElseThrow(() -> DingoException.from(DingoError.UNKNOWN));
    }

    private static String processKeyWords(String sql) {
        sql = io.dingodb.calcite.utils.StringEscapeUtils.unescape(sql);
        if (sql.endsWith(" ")) {
            sql = sql.trim();
        }
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        // for dump test
        if ((sql.startsWith("use") || sql.startsWith("USE")) && sql.contains("`") ) {
            sql = sql.replace("`", "");
        }
        // tmp todo replace
        if (sql.startsWith("/*!") && sql.endsWith("*/")) {
            sql = "set session net_write_timeout=10000";
        } else if (sql.contains("/*!") && sql.contains("*/")) {
            int beginIndex = sql.indexOf("/*!");
            int endIndex = sql.indexOf("*/");
            String comment = sql.substring(beginIndex, endIndex + 2);
            sql = sql.replace(comment, "");
        }
        // for dump test

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

    protected static boolean ddlTxn(SqlNode sqlNode) {
        return sqlNode instanceof DingoSqlCreateTable || sqlNode instanceof SqlTruncate
            || sqlNode instanceof SqlDropTable || sqlNode instanceof SqlCreateSchema
            || sqlNode instanceof SqlDropSchema || sqlNode instanceof SqlAlterAddColumn
            || sqlNode instanceof SqlAlterDropColumn || sqlNode instanceof SqlAlterAddIndex
            || sqlNode instanceof SqlAlterDropIndex || sqlNode instanceof SqlCreateUser
            || sqlNode instanceof SqlDropUser
            || sqlNode instanceof SqlGrant
            || sqlNode instanceof SqlAlterRenameTable
            || sqlNode instanceof SqlAlterRenameIndex
            || sqlNode instanceof SqlAlterChangeColumn
            || sqlNode instanceof SqlAlterModifyColumn
            || sqlNode instanceof SqlRecoverTable
            || sqlNode instanceof SqlFlashBackTable
            || sqlNode instanceof SqlFlashBackSchema
            || sqlNode instanceof SqlAlterColumn
            || sqlNode instanceof SqlAlterAutoIncrement
            || sqlNode instanceof SqlAdminResetAutoInc
            || sqlNode instanceof SqlAlterTableComment
            || sqlNode instanceof SqlAlterDropPart
            || sqlNode instanceof SqlAlterTruncatePart
            || sqlNode instanceof SqlAlterExchangePart
            || sqlNode instanceof SqlAlterTableOptions
            || sqlNode instanceof SqlAlterIndex
            || sqlNode instanceof SqlCreateSequence
            || sqlNode instanceof SqlDropSequence
            || sqlNode instanceof SqlAlterTableAddPart;
    }

    public long getGcLifeTime() {
        return Long.parseLong(InfoSchemaService.root().getGlobalVariables()
            .getOrDefault("safepoint_ts", "0"));
    }

    public SQLWarning getWarning(SqlNode sqlNode) {
        if (sqlNode instanceof SqlAlterAutoIncrement) {
            SqlAlterAutoIncrement alterAutoIncrement = (SqlAlterAutoIncrement) sqlNode;
            if (alterAutoIncrement.getWarning() != null) {
                return new SQLWarning(alterAutoIncrement.getWarning(), "1105");
            }
        }
        return null;
    }

}
