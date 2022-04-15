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
import io.dingodb.calcite.DingoConventions;
import io.dingodb.calcite.DingoParser;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoSchema;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.ddl.DingoDdlParserFactory;
import io.dingodb.exec.base.Job;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public final class DingoDriverParser extends DingoParser {
    public DingoDriverParser(DingoParserContext context) {
        super(context);
        parserConfig = SqlParser.config()
            .withParserFactory(DingoDdlParserFactory.INSTANCE);
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

    @Nonnull
    private static ColumnMetaData metaData(
        @Nonnull JavaTypeFactory typeFactory,
        int ordinal,
        String fieldName,
        @Nonnull RelDataType type,
        @Nullable List<String> origins
    ) {
        final ColumnMetaData.AvaticaType aType = ColumnMetaData.scalar(
            type.getSqlTypeName().getJdbcOrdinal(),
            fieldName,
            ColumnMetaData.Rep.of(typeFactory.getJavaClass(type))
        );
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
            aType,
            true,
            false,
            false,
            aType.columnClassName());
    }

    private static @Nullable String origin(@Nullable List<String> origins, int offsetFromEnd) {
        return origins == null || offsetFromEnd >= origins.size()
            ? null : origins.get(origins.size() - 1 - offsetFromEnd);
    }

    @Nonnull
    public DingoSignature parseQuery(String sql, CalcitePrepare.Context context) throws SqlParseException {
        SqlNode sqlNode = parse(sql);
        if (sqlNode.getKind().belongsTo(SqlKind.DDL)) {
            final DdlExecutor ddlExecutor = parserConfig.parserFactory().getDdlExecutor();
            ddlExecutor.executeDdl(context, sqlNode);
            return new DingoSignature(
                ImmutableList.of(),
                sql,
                Meta.CursorFactory.OBJECT,
                Meta.StatementType.OTHER_DDL
            );
        }
        JavaTypeFactory typeFactory = context.getTypeFactory();
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
        RelRoot relRoot = convert(sqlNode);
        RelNode relNode = optimize(relRoot.rel, DingoConventions.ROOT);
        CalciteSchema rootSchema = context.getRootSchema();
        CalciteSchema defaultSchema = rootSchema.getSubSchema(context.getDefaultSchemaPath().get(0), true);
        if (defaultSchema == null) {
            throw new RuntimeException("No default schema is found.");
        }
        Location currentLocation = ((DingoSchema) defaultSchema.schema).getMetaService().currentLocation();
        Job job = DingoJobVisitor.createJob(relNode, currentLocation, true);
        return new DingoSignature(
            columns,
            sql,
            null,
            null,
            cursorFactory,
            statementType,
            job
        );
    }
}
