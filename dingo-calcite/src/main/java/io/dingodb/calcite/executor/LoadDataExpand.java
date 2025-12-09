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

package io.dingodb.calcite.executor;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoSqlToRelConverter;
import io.dingodb.calcite.grammar.ddl.LoadDataColMapping;
import io.dingodb.calcite.grammar.ddl.LoadDataSetExpr;
import io.dingodb.calcite.meta.DingoRelMetadataProvider;
import io.dingodb.calcite.rel.DingoCost;
import io.dingodb.calcite.traits.DingoRelStreamingDef;
import io.dingodb.calcite.type.DingoSqlTypeFactory;
import io.dingodb.calcite.utils.CalcValueUtils;
import io.dingodb.common.table.HybridSearchTable;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.ParameterScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.InitializerContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class LoadDataExpand {
    List<LoadDataColMapping> withColumnList;
    Table table;
    MetaService metaService;
    List<Integer> userVarIndexList;
    List<String> realColList;
    LoadDataSetExpr setExpr;
    InitializerContext initializerContext;
    List<RexNode> sourceExprList;
    List<String> targetColList;

    public LoadDataExpand(
        Table table, List<LoadDataColMapping> withColumnList,
        LoadDataSetExpr setExpr, DingoParserContext dingoParserContext
    ) {
        this.table = table;
        metaService = MetaService.root();
        this.withColumnList = withColumnList;
        this.userVarIndexList = new ArrayList<>();
        if (this.withColumnList == null) {
            return;
        }
        for (int i = 0; i < withColumnList.size(); i ++) {
            if (withColumnList.get(i).isUserVar()) {
                userVarIndexList.add(i);
            }
        }
        this.realColList = withColumnList.stream()
            .filter(mapping -> !mapping.isUserVar())
            .map(LoadDataColMapping::getColumnName)
            .toList();
        this.setExpr = setExpr;
        this.initializerContext = getContext(dingoParserContext);
        if (setExpr != null) {
            initSetExpr(setExpr);
            targetColList = setExpr.getTargetColumnList()
                .stream().map(SqlNode::toString)
                .toList();
        }

    }

    private void initSetExpr(LoadDataSetExpr setExpr) {
        this.sourceExprList = setExpr.getSourceExpressionList()
            .stream().map(this::convertExpression)
            .toList();
    }

    public RexNode convertExpression(SqlNode sqlNode) {
        RexBuilder rexBuilder = new RexBuilder(DingoSqlTypeFactory.INSTANCE);
        RelDataType targetType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARBINARY);
        RexNode rex;
        if (sqlNode.getKind() == SqlKind.LITERAL && ((SqlLiteral) sqlNode).getValue() == null) {
            rex = rexBuilder.makeNullLiteral(targetType);
        } else if (sqlNode.getKind() == SqlKind.MULTISET_VALUE_CONSTRUCTOR) {
            assert sqlNode instanceof SqlCall;
            SqlCall call = (SqlCall) sqlNode;
            rex = rexBuilder.makeCall(
                SqlStdOperatorTable.MULTISET_VALUE,
                call.getOperandList().stream()
                    .map(this.initializerContext::convertExpression)
                    .collect(Collectors.toList())
            );
        } else {
            rex = this.initializerContext.convertExpression(sqlNode);
        }
        return rex;
    }

    private InitializerContext getContext(DingoParserContext context) {
        Map<String, RelDataType> nameToTypeMap = Collections.emptyMap();
        SqlValidator sqlValidator = context.getSqlValidator();
        final ParameterScope scope =
            new ParameterScope((SqlValidatorImpl) sqlValidator, nameToTypeMap);


        VolcanoPlanner planner = new VolcanoPlanner(DingoCost.FACTORY, context);
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
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(ChainedRelMetadataProvider.of(
            ImmutableList.of(
                DingoRelMetadataProvider.INSTANCE,
                Objects.requireNonNull(cluster.getMetadataProvider())
            )
        ));
        HintPredicate hintPredicate = (hint, rel) -> true;
        HintStrategyTable.Builder hintStrategyBuilder = new HintStrategyTable.Builder()
            .hintStrategy("vector_pre", hintPredicate)
            .hintStrategy(HybridSearchTable.HINT_NAME, hintPredicate)
            .hintStrategy("disable_index", hintPredicate)
            .hintStrategy("text_search_pre", hintPredicate);
        DingoSqlToRelConverter sqlToRelConverter = new DingoSqlToRelConverter(
            ViewExpanders.simpleContext(cluster),
            sqlValidator,
            context.getCatalogReader(),
            cluster,
            false,
            hintStrategyBuilder.build()
        );
        return sqlToRelConverter.new DingoBlackboard(scope, null, false, withColumnList);
    }

    public Object[] expand(Object[] tuples) {
        return columnMapping(tuples);
    }

    public Object[] columnMapping(Object[] tuples) {
        if (withColumnList == null) {
            return tuples;
        }
        if (withColumnList.size() != tuples.length) {
            return tuples;
        }
        Map<String, Object> colValMap = null;
        if (this.sourceExprList != null) {
            colValMap = expr(tuples);
        }

        Object[] tmp;
        if (!userVarIndexList.isEmpty()) {
            int newIndex = 0;
            tmp = new Object[tuples.length - userVarIndexList.size()];
            for (int i = 0; i < tuples.length; i ++) {
                if (!userVarIndexList.contains(i)) {
                    tmp[newIndex++] = tuples[i];
                }
            }
        } else {
            tmp = tuples;
        }
        List<Integer> columnIndices = table.getColumnIndices(realColList);
        Object[] tupleTmp = new Object[table.getColumns().size()];
        for (int i = 0; i < table.getColumns().size(); i ++) {
            Column column = table.getColumns().get(i);
            if (colValMap != null && colValMap.containsKey(column.getName())) {
                tupleTmp[i] = colValMap.get(column.getName());
                continue;
            }
            if (!columnIndices.contains(i)) {
                if (column.isAutoIncrement()) {
                    Long id = metaService.getAutoIncrement(table.getTableId());
                    tupleTmp[i] = id;
                } else if (!column.isNullable() && column.defaultValueExpr == null) {
                    tupleTmp[i] = null;
                } else {
                    tupleTmp[i] = column.defaultValueExpr;
                }
            } else {
                tupleTmp[i] = tmp[columnIndices.indexOf(i)];
            }
        }
        return tupleTmp;
    }

    static ExprConfig exprConfig = new ExprConfig() {
        @Override
        public boolean withRangeCheck() {
            return true;
        }

        @Override
        public TimeZone getTimeZone() {
            return TimeZone.getDefault();
        }

        @Override
        public DingoTimeZoneProcessor getProcessor() {
            return new DingoTimeZoneProcessor(TimeZone.getDefault().toZoneId());
        }
    };

    public Map<String, Object> expr(Object[] tuples) {
        if (this.sourceExprList == null || targetColList == null) {
            return null;
        }

        DingoType[] dingoTypes = new DingoType[tuples.length];
        for (int i = 0; i < tuples.length; i ++) {
            dingoTypes[i] = new StringType(false);
        }

        DingoType dingoType = new TupleType(dingoTypes);
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < sourceExprList.size(); i ++) {
            RexNode rexNode = sourceExprList.get(i);
            Object val;
            try {
                val = CalcValueUtils.calcValue(rexNode, tuples, dingoType, exprConfig);
            } catch (Exception ignore) {
                val = null;
            }
            String colNm = targetColList.get(i);
            map.put(colNm, val);
        }
        return map;
    }
}
