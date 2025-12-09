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

import io.dingodb.calcite.fun.DingoOperatorTable;
import io.dingodb.common.mysql.DingoErrUtil;
import io.dingodb.common.mysql.SQLMode;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.sql.DingoSqlBasicCall;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.AggregatingSelectScope;
import org.apache.calcite.sql.validate.SqlNonNullableAccessors;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.TableDiskAnnFunctionNamespace;
import org.apache.calcite.sql.validate.TableFunctionNamespace;
import org.apache.calcite.sql.validate.TableGenerateSeriesFunctionNamespace;
import org.apache.calcite.sql.validate.TableHybridFunctionNamespace;
import org.apache.calcite.sql.validate.implicit.DingoTypeCoercionImpl;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.sql2rel.SqlDiskAnnOperator;
import org.apache.calcite.sql2rel.SqlDocumentOperator;
import org.apache.calcite.sql2rel.SqlFunctionScanOperator;
import org.apache.calcite.sql2rel.SqlGenerateSeriesOperator;
import org.apache.calcite.sql2rel.SqlHybridSearchOperator;
import org.apache.calcite.sql2rel.SqlVectorOperator;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.common.mysql.error.ErrorCode.ErrFieldNotInGroupBy;
import static io.dingodb.common.mysql.error.ErrorCode.ErrMixOfGroupFuncAndFields;
import static org.apache.calcite.util.Static.RESOURCE;

public class DingoSqlValidator extends SqlValidatorImpl {

    @Getter
    @Setter
    private boolean hybridSearch;
    @Getter
    @Setter
    private String hybridSearchSql;
    @Getter
    private Map<SqlBasicCall, String> hybridSearchMap;

    private TypeCoercion typeCoercion1;

    static Config CONFIG = Config.DEFAULT
        .withTypeCoercionFactory(DingoSqlValidator::createTypeCoercion)
        .withConformance(DingoParser.PARSER_CONFIG.conformance());

    public static final Set<SqlKind> AGGREGATE_KIND =
        EnumSet.of(SqlKind.SUM, SqlKind.SUM0, SqlKind.AVG, SqlKind.COUNT, SqlKind.MIN, SqlKind.MAX);

    public static TypeCoercion createTypeCoercion(RelDataTypeFactory typeFactory,
                                                  SqlValidator validator) {
        return new DingoTypeCoercionImpl(typeFactory, validator);
    }

    DingoSqlValidator(
        DingoCatalogReader catalogReader,
        RelDataTypeFactory typeFactory
    ) {
        super(
            SqlOperatorTables.chain(
                SqlStdOperatorTable.instance(),
                DingoOperatorTable.instance(),
                catalogReader
            ),
            catalogReader,
            typeFactory,
            DingoSqlValidator.CONFIG
        );
        this.hybridSearch = false;
        this.hybridSearchSql = "";
        this.hybridSearchMap = new ConcurrentHashMap<>();
        TypeCoercion typeCoercion2 = CONFIG.typeCoercionFactory().create(typeFactory, this);
        this.typeCoercion1 = typeCoercion2;
    }

    @Override
    public void validateCall(SqlCall call, SqlValidatorScope scope) {
        super.validateCall(call, scope);
    }

    @Override
    protected void registerNamespace(
        @Nullable SqlValidatorScope usingScope, @Nullable String alias, SqlValidatorNamespace ns, boolean forceNullable
    ) {
        SqlNode enclosingNode = ns.getEnclosingNode();
        if (enclosingNode instanceof SqlBasicCall
            && (((SqlBasicCall) enclosingNode).getOperator() instanceof SqlFunctionScanOperator
            || ((SqlBasicCall) enclosingNode).getOperator() instanceof SqlVectorOperator
            || ((SqlBasicCall) enclosingNode).getOperator() instanceof SqlDocumentOperator)
        ) {
            super.registerNamespace(
                usingScope, alias,
                new TableFunctionNamespace(this, (SqlBasicCall) enclosingNode),
                forceNullable
            );
            return;
        } else if (enclosingNode instanceof SqlBasicCall
            && (((SqlBasicCall) enclosingNode).getOperator() instanceof SqlHybridSearchOperator)
        ) {
            super.registerNamespace(
                usingScope, alias,
                new TableHybridFunctionNamespace(this, (SqlBasicCall) enclosingNode),
                forceNullable
            );
            return;
        } else if (enclosingNode instanceof SqlBasicCall
            && (((SqlBasicCall) enclosingNode).getOperator() instanceof SqlDiskAnnOperator)
        ) {
            super.registerNamespace(
                usingScope, alias,
                new TableDiskAnnFunctionNamespace(this, (SqlBasicCall) enclosingNode),
                forceNullable
            );
            return;
        } else if (enclosingNode instanceof SqlBasicCall
            && (((SqlBasicCall) enclosingNode).getOperator() instanceof SqlGenerateSeriesOperator)
        ) {
            super.registerNamespace(
                usingScope, alias,
                new TableGenerateSeriesFunctionNamespace(this, (SqlBasicCall) enclosingNode),
                forceNullable
            );
            return;
        }
        super.registerNamespace(usingScope, alias, ns, forceNullable);
    }

    @Override
    public @Nullable SqlValidatorNamespace getNamespace(SqlNode node) {
        switch (node.getKind()) {
            case COLLECTION_TABLE:
                return namespaces.get(node);
            default:
                return super.getNamespace(node);
        }
    }

    @Override
    protected void validateTableFunction(SqlCall node, SqlValidatorScope scope, RelDataType targetRowType) {
        validateQuery(node, scope, targetRowType);
    }

    @Override
    protected void inferUnknownTypes(RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
        if (node instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) node;
            if (sqlBasicCall.getOperator() instanceof SqlMapValueConstructor) {
                SqlMapValueConstructor mapValueConstructor = (SqlMapValueConstructor) sqlBasicCall.getOperator();
                if ("MAP".equalsIgnoreCase(mapValueConstructor.getName())) {
                    return;
                }
            }
        }
        super.inferUnknownTypes(inferredType, scope, node);
    }

    protected void validateValues(
        SqlCall node,
        RelDataType targetRowType,
        final SqlValidatorScope scope) {
        assert node.getKind() == SqlKind.VALUES;

        final List<SqlNode> operands = node.getOperandList();
        for (SqlNode operand : operands) {
            if (!(operand.getKind() == SqlKind.ROW)) {
                throw Util.needToImplement(
                    "Values function where operands are scalars");
            }

            SqlCall rowConstructor = (SqlCall) operand;
            if (false
                && targetRowType.isStruct()
                && rowConstructor.operandCount() < targetRowType.getFieldCount()) {
                targetRowType =
                    typeFactory.createStructType(
                        targetRowType.getFieldList()
                            .subList(0, rowConstructor.operandCount()));
            } else if (targetRowType.isStruct()
                && rowConstructor.operandCount() != targetRowType.getFieldCount()) {
                return;
            }

            inferUnknownTypes(
                targetRowType,
                scope,
                rowConstructor);

            final SqlNode top = getTop();
            boolean isIgnore = false;
            if (top instanceof io.dingodb.calcite.grammar.dml.SqlInsert) {
                io.dingodb.calcite.grammar.dml.SqlInsert sqlInsert = (io.dingodb.calcite.grammar.dml.SqlInsert) top;
                isIgnore = sqlInsert.isIgnore();
            }
            if (!isIgnore) {
                checkNullable(node, targetRowType, rowConstructor);
            }
        }

        for (SqlNode operand : operands) {
            operand.validate(this, scope);
        }

        // validate that all row types have the same number of columns
        //  and that expressions in each column are compatible.
        // A values expression is turned into something that looks like
        // ROW(type00, type01,...), ROW(type11,...),...
        final int rowCount = operands.size();
        if (rowCount >= 2) {
            SqlCall firstRow = (SqlCall) operands.get(0);
            final int columnCount = firstRow.operandCount();

            // 1. check that all rows have the same cols length
            for (SqlNode operand : operands) {
                SqlCall thisRow = (SqlCall) operand;
                if (columnCount != thisRow.operandCount()) {
                    throw newValidationError(node,
                        RESOURCE.incompatibleValueType(
                            SqlStdOperatorTable.VALUES.getName()));
                }
            }

            // 2. check if types at i:th position in each row are compatible
            for (int col = 0; col < columnCount; col++) {
                final int c = col;
                final RelDataType type =
                    typeFactory.leastRestrictive(
                        new AbstractList<RelDataType>() {
                            @Override public RelDataType get(int row) {
                                SqlCall thisRow = (SqlCall) operands.get(row);
                                return deriveType(scope, thisRow.operand(c));
                            }

                            @Override public int size() {
                                return rowCount;
                            }
                        });

                if (null == type) {
                    throw newValidationError(node,
                        RESOURCE.incompatibleValueType(
                            SqlStdOperatorTable.VALUES.getName()));
                }
            }
        }
    }

    private void checkNullable(SqlCall node, RelDataType targetRowType, SqlCall rowConstructor) {
        if (targetRowType.isStruct()) {
            for (Pair<SqlNode, RelDataTypeField> pair
                : Pair.zip(rowConstructor.getOperandList(),
                targetRowType.getFieldList())) {
                if (!pair.right.getType().isNullable()
                    && SqlUtil.isNullLiteral(pair.left, false)) {
                    throw newValidationError(node,
                        RESOURCE.columnNotNullable(pair.right.getName()));
                }
            }
        }
    }

    public void checkTypeAssignment(
        @Nullable SqlValidatorScope sourceScope,
        SqlValidatorTable table,
        RelDataType sourceRowType,
        RelDataType targetRowType,
        final SqlNode query) {
        // NOTE jvs 23-Feb-2006: subclasses may allow for extra targets
        // representing system-maintained columns, so stop after all sources
        // matched
        boolean isUpdateModifiableViewTable = false;
        if (query instanceof SqlUpdate) {
            final SqlNodeList targetColumnList = ((SqlUpdate) query).getTargetColumnList();
            if (targetColumnList != null) {
                final int targetColumnCnt = targetColumnList.size();
                targetRowType = SqlTypeUtil.extractLastNFields(typeFactory, targetRowType,
                    targetColumnCnt);
                sourceRowType = SqlTypeUtil.extractLastNFields(typeFactory, sourceRowType,
                    targetColumnCnt);
            }
            isUpdateModifiableViewTable = table.unwrap(ModifiableViewTable.class) != null;
        }
        if (SqlTypeUtil.equalAsStructSansNullability(typeFactory,
            sourceRowType,
            targetRowType,
            null)) {
            // Returns early if source and target row type equals sans nullability.
            return;
        }

        //Check precison for type cast.
        List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
        List<RelDataTypeField> targetFields = targetRowType.getFieldList();
        final int sourceCount = sourceFields.size();

        if (CONFIG.typeCoercionEnabled() && !isUpdateModifiableViewTable) {
            // Try type coercion first if implicit type coercion is allowed.
            boolean coerced = typeCoercion1.querySourceCoercion(sourceScope,
                sourceRowType,
                targetRowType,
                query);
            if (coerced) {
                return;
            }
        }

        // Fall back to default behavior: compare the type families.
        for (int i = 0; i < sourceCount; ++i) {
            RelDataTypeField sourceTypeField = sourceFields.get(i);
            if (IMPLICIT_COL_NAME.equals(sourceTypeField.getName())) {
                continue;
            }
            RelDataType sourceType = sourceTypeField.getType();
            RelDataType targetType = targetFields.get(i).getType();
            if (!SqlTypeUtil.canAssignFrom(targetType, sourceType)) {
                SqlNode node = getNthExpr(query, i, sourceCount);
                if (node instanceof SqlDynamicParam) {
                    continue;
                }

                if (sourceType.getSqlTypeName() == SqlTypeName.VARCHAR
                    && (targetType.getSqlTypeName() == SqlTypeName.TIME
                    || targetType.getSqlTypeName() == SqlTypeName.TIMESTAMP
                    || targetType.getSqlTypeName() == SqlTypeName.DATE)) {
                    continue;
                }

                String targetTypeString;
                String sourceTypeString;
                if (SqlTypeUtil.areCharacterSetsMismatched(
                    sourceType,
                    targetType)) {
                    sourceTypeString = sourceType.getFullTypeString();
                    targetTypeString = targetType.getFullTypeString();
                } else {
                    sourceTypeString = sourceType.toString();
                    targetTypeString = targetType.toString();
                }
                throw newValidationError(node,
                    RESOURCE.typeNotAssignable(
                        targetFields.get(i).getName(), targetTypeString,
                        sourceFields.get(i).getName(), sourceTypeString));
            }
        }
    }

    private static SqlNode getNthExpr(SqlNode query, int ordinal, int sourceCount) {
        if (query instanceof SqlInsert) {
            SqlInsert insert = (SqlInsert) query;
            if (insert.getTargetColumnList() != null) {
                return insert.getTargetColumnList().get(ordinal);
            } else {
                return getNthExpr(
                    insert.getSource(),
                    ordinal,
                    sourceCount);
            }
        } else if (query instanceof SqlUpdate) {
            SqlUpdate update = (SqlUpdate) query;
            if (update.getSourceExpressionList() != null) {
                return update.getSourceExpressionList().get(ordinal);
            } else {
                return getNthExpr(
                    SqlNonNullableAccessors.getSourceSelect(update),
                    ordinal,
                    sourceCount);
            }
        } else if (query instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) query;
            SqlNodeList selectList = SqlNonNullableAccessors.getSelectList(select);
            if (selectList.size() == sourceCount) {
                return selectList.get(ordinal);
            } else {
                return query; // give up
            }
        } else {
            return query; // give up
        }
    }

    public void validateLiteral(SqlLiteral literal) {
        switch (literal.getTypeName()) {
            case DECIMAL:
                //BigDecimal bd = (BigDecimal)literal.getValueAs(BigDecimal.class);
                //BigInteger unscaled = bd.toBigInteger();
                //long longValue = unscaled.longValue();
                //if (!BigInteger.valueOf(longValue).equals(unscaled)) {
                //    throw this.newValidationError(literal, Static.RESOURCE.numberLiteralOutOfRange(bd.toString()));
                //}
                break;
            case DOUBLE:
                this.validateLiteralAsDouble(literal);
                break;
            case BINARY:
                BitString bitString = (BitString)literal.getValueAs(BitString.class);
                if (bitString.getBitCount() % 8 != 0) {
                    throw this.newValidationError(literal, RESOURCE.binaryLiteralOdd());
                }
                break;
            case DATE:
            case TIME:
            case TIMESTAMP:
                Calendar calendar = (Calendar)literal.getValueAs(Calendar.class);
                int year = calendar.get(1);
                int era = calendar.get(0);
                if (year < 1 || era == 0 || year > 9999) {
                    throw this.newValidationError(literal, RESOURCE.dateLiteralOutOfRange(literal.toString()));
                }
                break;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                if (literal instanceof SqlIntervalLiteral) {
                    SqlIntervalLiteral.IntervalValue interval
                        = (SqlIntervalLiteral.IntervalValue)literal.getValueAs(SqlIntervalLiteral.IntervalValue.class);
                    SqlIntervalQualifier intervalQualifier = interval.getIntervalQualifier();
                    // this.validateIntervalQualifier(intervalQualifier);
                    String intervalStr = interval.getIntervalLiteral();
                    int[] values = intervalQualifier.evaluateIntervalLiteral(
                        intervalStr, literal.getParserPosition(), this.typeFactory.getTypeSystem()
                    );
                    Util.discard(values);
                }
        }
    }

    private void validateLiteralAsDouble(SqlLiteral literal) {
        BigDecimal bd = (BigDecimal)literal.getValueAs(BigDecimal.class);
        double d = bd.doubleValue();
        if (Double.isInfinite(d) || Double.isNaN(d)) {
            throw this.newValidationError(literal, RESOURCE.numberLiteralOutOfRange(Util.toScientificNotation(bd)));
        }
    }

    @Override public SqlNode expandOrderExpr(SqlSelect select, SqlNode orderExpr) {
        SqlNode resNode = super.expandOrderExpr(select, orderExpr);
        if (!(resNode instanceof DingoSqlBasicCall) && resNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) resNode;
            return new DingoSqlBasicCall(sqlBasicCall.getOperator(), sqlBasicCall.getOperandList(),
                sqlBasicCall.getParserPosition(), sqlBasicCall.getFunctionQuantifier());
        } else {
            return resNode;
        }
    }

    public void expandSelectItemWithNotInGroupBy(List<SqlNode> selectItems, SqlValidatorScope scope) {
        if (!(scope instanceof AggregatingSelectScope)) {
            return;
        }
        AggregatingSelectScope aggScope = (AggregatingSelectScope) scope;
        boolean itemAggregateCall = false;
        boolean hasNotGroupExpr = false;
        boolean groupExprsEmpty = aggScope.getGroupExprs() != null && aggScope.getGroupExprs().getKey().isEmpty()
            && aggScope.getGroupExprs().getValue().isEmpty();
        int errIndex = -1;
        String errCol = "";
        for (int i = 0; i < selectItems.size(); i++) {
            SqlNode item = selectItems.get(i);
            if (item instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) item;
                if (sqlBasicCall.isA(AGGREGATE_KIND)) {
                    itemAggregateCall = true;
                }
            }
            if (aggScope.isNotGroupExpr(item)) {
                hasNotGroupExpr = true;
                SqlNode sqlNode = null;
                SqlNode sqlNodeAs = null;
                if (item.getKind() == SqlKind.AS) {
                    SqlCall as = (SqlCall) item;
                    sqlNode = as.operand(0);
                    sqlNodeAs = as.operand(1);
                } else {
                    sqlNode = item;
                    sqlNodeAs = new SqlIdentifier(SqlValidatorUtil.getAlias(item, 0), SqlParserPos.ZERO);
                }
                if (groupExprsEmpty) {
                    if (itemAggregateCall) {
                        throw DingoErrUtil.newStdErr(ErrMixOfGroupFuncAndFields, i + 1, item.toString());
                    }
                    final SqlNode newNode = aggScope.replaceNotGroupExpr(sqlNode);
                    selectItems.set(i, SqlStdOperatorTable.AS.createCall(newNode.getParserPosition(),
                        newNode, new SqlIdentifier(SqlValidatorUtil.getAlias(sqlNodeAs, 0), SqlParserPos.ZERO)));
                    errIndex = i;
                    errCol = item.toString();
                } else if (this.getCatalogReader() instanceof DingoCatalogReader) {
                    DingoCatalogReader dingoCatalogReader = (DingoCatalogReader) this.getCatalogReader();
                    if (SQLMode.isOnlyFullGroupBy(dingoCatalogReader.getSqlModeFlags())
                        && !sqlNode.toString().toUpperCase().contains(IMPLICIT_COL_NAME)) {
                        throw DingoErrUtil.newStdErr(ErrFieldNotInGroupBy, i + 1, "SELECT list", item.toString());
                    } else {
                        final SqlNode newNode = aggScope.replaceNotGroupExpr(sqlNode);
                        selectItems.set(i, SqlStdOperatorTable.AS.createCall(newNode.getParserPosition(),
                            newNode, new SqlIdentifier(SqlValidatorUtil.getAlias(sqlNodeAs, 0), SqlParserPos.ZERO)));
                    }
                }

            }
        }
        if (groupExprsEmpty && itemAggregateCall && hasNotGroupExpr) {
            throw DingoErrUtil.newStdErr(ErrMixOfGroupFuncAndFields, errIndex + 1, errCol);
        }
    }
}
