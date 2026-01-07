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
import io.dingodb.calcite.rel.LogicalDingoTableScan;
import io.dingodb.calcite.rel.LogicalForUpdate;
import io.dingodb.calcite.schema.SubSnapshotSchema;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.HybridNodeUtils;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.common.table.HybridSearchTable;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NameCaseUtils.convertName;

@Slf4j
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
public class DingoTable extends AbstractTable implements TranslatableTable {

    @Getter
    private final DingoParserContext context;
    @Getter
    private final List<String> names;

    @Getter
    @EqualsAndHashCode.Include
    private final Table table;
    @Getter
    private final List<IndexTable> indexTableDefinitions;

    public DingoTable(
        @NonNull DingoParserContext context,
        @NonNull List<String> names,
        @NonNull Table table
    ) {
        super();
        this.context = context;
        this.names = names;
        this.table = table;
        this.indexTableDefinitions = table.getIndexes();
    }

    public CommonId getTableId() {
        return table.getTableId();
    }

    public SubSnapshotSchema getSchema() {
        try {
            return (SubSnapshotSchema) context.getSchemaByNames(names).schema;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            return null;
        }
    }

    public IndexTable getIndexDefinition(String name) {
        return indexTableDefinitions.stream()
            .filter(i -> i.getName().equalsIgnoreCase(name))
            .findAny()
            .orElse(null);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return DefinitionMapper.mapToRelDataType(table, typeFactory);
    }

    @Override
    public RelNode toRel(RelOptTable.@NonNull ToRelContext context, RelOptTable relOptTable) {
        DingoTable dingoTable = relOptTable.unwrap(DingoTable.class);
        boolean forUpdate = context.getCluster()
            .getHintStrategies()
            .validateHint(RelHint.builder("for_update").build());
        if (dingoTable.getTable().getTableType() == null
            || (!dingoTable.getTable().getTableType().equalsIgnoreCase("VIEW"))
            || convertName(dingoTable.getSchema().getSchemaName())
            .equals(convertName("INFORMATION_SCHEMA"))) {
            LogicalDingoTableScan logicalDingoTableScan = new LogicalDingoTableScan(
                context.getCluster(),
                context.getCluster().traitSet(),
                context.getTableHints(),
                relOptTable,
                null,
                null,
                null,
                null,
                null,
                ((DingoParserContext) context.getCluster().getPlanner().getContext()).isPushDown(),
                false
            );
            if (forUpdate) {
                return new LogicalForUpdate(
                    context.getCluster(),
                    context.getCluster().traitSet(),
                    logicalDingoTableScan,
                    relOptTable);
            }
            return logicalDingoTableScan;
        } else {
            SqlParser parser = SqlParser.create(dingoTable.getTable().createSql, DingoParser.PARSER_CONFIG);
            try {
                SqlNode sqlNode = parser.parseQuery();
                String viewSchema = dingoTable.getTable().getProperties().getProperty("envSchema");

                DingoParserContext dingoParserContext;
                if (viewSchema == null) {
                    dingoParserContext = this.context;
                } else {
                    dingoParserContext = new DingoParserContext(viewSchema,
                        this.context.getOptions(), this.context.getSessionVariables(), this.context.getTimeZone());
                }
                DingoSqlValidator dingoSqlValidator = dingoParserContext.getSqlValidator();
                sqlNode = dingoSqlValidator.validate(sqlNode);

                if (dingoSqlValidator.isHybridSearch()) {
                    SqlNode originalSqlNode;
                    parser = SqlParser.create(dingoTable.getTable().createSql, DingoParser.PARSER_CONFIG);
                    originalSqlNode = parser.parseQuery();
                    //syntacticSugar(originalSqlNode);
                    if (dingoSqlValidator.getHybridSearchMap().size() == 1) {
                        String hybridSearchSql = dingoSqlValidator.getHybridSearchSql();
                        LogUtils.info(log, "HybridSearchSql: {}", hybridSearchSql);
                        SqlNode hybridSqlNode;
                        parser = SqlParser.create(hybridSearchSql, DingoParser.PARSER_CONFIG);
                        hybridSqlNode = parser.parseQuery();
                        //syntacticSugar(hybridSqlNode);
                        HybridNodeUtils.lockUpHybridSearchNode(originalSqlNode, hybridSqlNode);
                    } else {
                        ConcurrentHashMap<SqlBasicCall, SqlNode> sqlNodeHashMap = new ConcurrentHashMap<>();
                        for (Map.Entry<SqlBasicCall, String> entry : dingoSqlValidator
                            .getHybridSearchMap().entrySet()) {
                            SqlBasicCall key = entry.getKey();
                            String value = entry.getValue();
                            SqlNode hybridSqlNode;
                            parser = SqlParser.create(value, DingoParser.PARSER_CONFIG);
                            hybridSqlNode = parser.parseQuery();
                            //syntacticSugar(hybridSqlNode);
                            sqlNodeHashMap.put(key, hybridSqlNode);
                        }
                        HybridNodeUtils.lockUpHybridSearchNode(originalSqlNode, sqlNodeHashMap);
                    }
                    //LogUtils.info(log, "HybridSearch Rewrite Sql: {}", originalSqlNode.toString());
                    try {
                        sqlNode = dingoSqlValidator.validate(originalSqlNode);
                    } catch (CalciteContextException e) {
                        LogUtils.error(log, "HybridSearch parse and validate error, sql: <[{}]>.",
                            table.getCreateSql(), e);
                        throw e;
                    }
                }
                HintPredicate hintPredicate = (hint, rel) -> true;
                HintStrategyTable hintStrategyTable = new HintStrategyTable.Builder()
                    .hintStrategy("vector_pre", hintPredicate)
                    .hintStrategy(HybridSearchTable.HINT_NAME, hintPredicate)
                    .hintStrategy("disable_index", hintPredicate)
                    .hintStrategy("text_search_pre", hintPredicate)
                    .build();
                SqlToRelConverter sqlToRelConverter = new DingoSqlToRelConverter(
                    ViewExpanders.simpleContext(context.getCluster()),
                    dingoSqlValidator,
                    this.context.getCatalogReader(),
                    context.getCluster(),
                    sqlNode.getKind() == SqlKind.EXPLAIN,
                    hintStrategyTable
                );

                return sqlToRelConverter.convertQuery(sqlNode, false, true).rel;
            } catch (SqlParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Statistic getStatistic() {
        List<Integer> keyIndices = Arrays.stream(table.keyMapping().getMappings())
            .boxed()
            .collect(Collectors.toList());
        List<ImmutableBitSet> keys = ImmutableList.of(ImmutableBitSet.of(keyIndices));
        return new Statistic() {
            @Override
            public Double getRowCount() {
                return ScopeVariables.getStatsDefaultCount();
            }

            @Override
            public boolean isKey(ImmutableBitSet columns) {
                if (columns == null) {
                    return false;
                }
                return keys.stream().allMatch(set -> {
                    if (set == null) {
                        return false;
                    }
                    if (set.isEmpty() && !columns.isEmpty()) {
                        return false;
                    } else {
                        return columns.contains(set);
                    }
                });
            }

            @Override
            public List<ImmutableBitSet> getKeys() {
                return keys;
            }

            @Override
            public RelDistribution getDistribution() {
                return RelDistributions.hash(keyIndices);
            }
        };
    }

    @Override
    public <C> @Nullable C unwrap(@NonNull Class<C> clazz) {
        if (clazz.isAssignableFrom(InitializerExpressionFactory.class)) {
            return clazz.cast(DingoInitializerExpressionFactory.INSTANCE);
        } else if (clazz.isAssignableFrom(Prepare.PreparingTable.class)) {
            return clazz.cast(new DingoRelOptTable(this, context.getOption("user"),
                context.getOption("host")));
        }
        return super.unwrap(clazz);
    }
}
