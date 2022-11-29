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

package io.dingodb.driver;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.DingoDdlParserFactory;
import io.dingodb.calcite.DingoParser;
import io.dingodb.calcite.DingoSchema;
import io.dingodb.calcite.MetaCache;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

@Slf4j
public final class DingoDriverParser extends DingoParser {
    private final DingoConnection connection;

    public DingoDriverParser(@Nonnull DingoConnection connection) {
        super(connection.getContext(), connection.config());
        this.connection = connection;
    }

    private static RelDataType makeStruct(RelDataTypeFactory typeFactory, @Nonnull RelDataType type) {
        if (type.isStruct()) {
            return type;
        }
        return typeFactory.builder().add("$0", type).build();
    }

    @Nonnull
    private static List<ColumnMetaData> getColumnMetaDataList(
        JavaTypeFactory typeFactory,
        @Nonnull RelDataType jdbcType,
        List<? extends @Nullable List<String>> originList
    ) {
        List<RelDataTypeField> fieldList = jdbcType.getFieldList();
        final List<ColumnMetaData> columns = new ArrayList<>(fieldList.size());
        for (int i = 0; i < fieldList.size(); ++i) {
            RelDataTypeField field = fieldList.get(i);
            columns.add(metaData(
                typeFactory,
                columns.size(),
                field.getName(),
                field.getType(),
                originList.get(i)
            ));
        }
        return columns;
    }

    public static ColumnMetaData.AvaticaType avaticaType(
        @Nonnull JavaTypeFactory typeFactory,
        @Nonnull RelDataType type
    ) {
        SqlTypeName typeName = type.getSqlTypeName();
        switch (typeName) {
            case ARRAY:
            case MULTISET:
                return ColumnMetaData.array(
                    avaticaType(typeFactory, Objects.requireNonNull(type.getComponentType())),
                    type.getSqlTypeName().getName(),
                    ColumnMetaData.Rep.of(typeFactory.getJavaClass(type))
                );
            default:
                return ColumnMetaData.scalar(
                    type.getSqlTypeName().getJdbcOrdinal(),
                    type.getSqlTypeName().getName(),
                    ColumnMetaData.Rep.of(typeFactory.getJavaClass(type))
                );
        }
    }

    @Nonnull
    private static ColumnMetaData metaData(
        @Nonnull JavaTypeFactory typeFactory,
        int ordinal,
        String fieldName,
        @Nonnull RelDataType type,
        @Nullable List<String> origins
    ) {
        ColumnMetaData.AvaticaType avaticaType = avaticaType(typeFactory, type);
        return new ColumnMetaData(
            ordinal,
            false,
            true,
            false,
            false,
            type.isNullable() ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
            true,
            type.getPrecision(),
            fieldName,
            origin(origins, 0),
            origin(origins, 2),
            type.getPrecision(),
            type.getScale(),
            origin(origins, 1),
            null,
            avaticaType,
            true,
            false,
            false,
            avaticaType.columnClassName()
        );
    }

    private static @Nullable String origin(@Nullable List<String> origins, int offsetFromEnd) {
        return origins == null || offsetFromEnd >= origins.size()
            ? null : origins.get(origins.size() - 1 - offsetFromEnd);
    }

    @Nonnull
    private static List<AvaticaParameter> createParameterList(@Nonnull RelDataType parasType) {
        List<RelDataTypeField> fieldList = parasType.getFieldList();
        final List<AvaticaParameter> parameters = new ArrayList<>(fieldList.size());
        for (RelDataTypeField field : fieldList) {
            RelDataType type = field.getType();
            parameters.add(
                new AvaticaParameter(
                    false,
                    type.getPrecision(),
                    type.getScale(),
                    type.getSqlTypeName().getJdbcOrdinal(),
                    type.getSqlTypeName().toString(),
                    Object.class.getName(),
                    field.getName()));
        }
        return parameters;
    }

    @Nonnull
    public Meta.Signature parseQuery(
        JobManager jobManager,
        Id jobId,
        String sql
    ) throws SqlParseException {
        MetaCache.initTableDefinitions();
        SqlNode sqlNode = parse(sql);
        if (sqlNode.getKind().belongsTo(SqlKind.DDL)) {
            final DdlExecutor ddlExecutor = PARSER_CONFIG.parserFactory().getDdlExecutor();
            ddlExecutor.executeDdl(connection, sqlNode);
            return new DingoSignature(
                ImmutableList.of(),
                sql,
                Meta.CursorFactory.OBJECT,
                Meta.StatementType.OTHER_DDL
            );
        }
        JavaTypeFactory typeFactory = connection.getTypeFactory();
        SqlExplain explain = null;
        if (sqlNode.getKind().equals(SqlKind.EXPLAIN)) {
            explain = (SqlExplain) sqlNode;
            sqlNode = explain.getExplicandum();
        }
        sqlNode = validate(sqlNode);
        Meta.StatementType statementType;
        RelDataType type;
        switch (sqlNode.getKind()) {
            case INSERT:
            case DELETE:
            case UPDATE:
                statementType = Meta.StatementType.IS_DML;
                type = RelOptUtil.createDmlRowType(sqlNode.getKind(), typeFactory);
                break;
            default:
                statementType = Meta.StatementType.SELECT;
                type = getValidatedNodeType(sqlNode);
                break;
        }
        RelDataType jdbcType = makeStruct(typeFactory, type);
        List<List<String>> originList = getFieldOrigins(sqlNode);
        final List<ColumnMetaData> columns = getColumnMetaDataList(typeFactory, jdbcType, originList);
        final Meta.CursorFactory cursorFactory = Meta.CursorFactory.ARRAY;
        final RelRoot relRoot = convert(sqlNode, false);
        final RelNode relNode = optimize(relRoot.rel);
        CalciteSchema rootSchema = connection.getRootSchema();
        CalciteSchema defaultSchema = rootSchema.getSubSchema(connection.getDefaultSchemaPath().get(0), true);
        if (defaultSchema == null) {
            throw new RuntimeException("No default schema is found.");
        }
        Location currentLocation = ((DingoSchema) defaultSchema.schema).getMetaService().currentLocation();
        RelDataType parasType = getParameterRowType(sqlNode);
        Job job = jobManager.createJob(jobId, DefinitionMapper.mapToDingoType(parasType));
        DingoJobVisitor.renderJob(job, relNode, currentLocation, true);
        if (explain != null) {
            statementType = Meta.StatementType.CALL;
            String logicalPlan = RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
            return new DingoExplainSignature(
                ImmutableList.of(metaData(typeFactory, 0, "PLAN",
                    new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.CHAR), null)),
                sql,
                createParameterList(parasType),
                null,
                cursorFactory,
                statementType,
                sqlNode.toString(),
                logicalPlan,
                job
            );
        }
        return new DingoSignature(
            columns,
            sql,
            createParameterList(parasType),
            null,
            cursorFactory,
            statementType
        );
    }
}
