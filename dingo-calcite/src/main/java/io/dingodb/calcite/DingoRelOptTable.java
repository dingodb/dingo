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

import io.dingodb.common.CommonId;
import io.dingodb.meta.SysInfoService;
import io.dingodb.meta.SysInfoServiceProvider;
import io.dingodb.verify.privilege.PrivilegeVerify;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessEnum;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.EnumSet;
import java.util.List;

public class DingoRelOptTable extends Prepare.AbstractPreparingTable {
    private final RelOptTableImpl relOptTable;

    private static SysInfoService sysInfoService;

    private String user;

    private String host;

    DingoRelOptTable(@NonNull DingoTable table, String user, String host) {
        super();
        DingoParserContext context = table.getContext();
        RelOptSchema relOptSchema = context.getCatalogReader().unwrap(RelOptSchema.class);
        final RelDataType rowType = table.getRowType(context.getTypeFactory());
        relOptTable = RelOptTableImpl.create(relOptSchema, rowType, table.getNames(), table, null);
        this.user = user;
        this.host = host;
    }

    @Override
    protected RelOptTable extend(Table extendedTable) {
        // Seems not called in any case.
        return null;
    }

    @Override
    public List<String> getQualifiedName() {
        return relOptTable.getQualifiedName();
    }

    @Override
    public double getRowCount() {
        return relOptTable.getRowCount();
    }

    @Override
    public RelDataType getRowType() {
        return relOptTable.getRowType();
    }

    @Override
    public @Nullable RelOptSchema getRelOptSchema() {
        return relOptTable.getRelOptSchema();
    }

    @Override
    public RelNode toRel(ToRelContext context) {
        return relOptTable.toRel(context);
    }

    @Override
    public @Nullable List<RelCollation> getCollationList() {
        return relOptTable.getCollationList();
    }

    @Override
    public @Nullable RelDistribution getDistribution() {
        return relOptTable.getDistribution();
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return relOptTable.isKey(columns);
    }

    @Override
    public @Nullable List<ImmutableBitSet> getKeys() {
        return relOptTable.getKeys();
    }

    @Override
    public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
        return relOptTable.getReferentialConstraints();
    }

    @Override
    public @Nullable Expression getExpression(Class clazz) {
        return relOptTable.getExpression(clazz);
    }

    @Override
    public SqlMonotonicity getMonotonicity(String columnName) {
        return relOptTable.getMonotonicity(columnName);
    }

    @Override
    public SqlAccessType getAllowedAccess() {
        List<String> names = relOptTable.getQualifiedName();
        if (StringUtils.isBlank(user)) {
            return SqlAccessType.ALL;
        } else {
            String catalog = null;
            String schema = null;
            String table = null;
            if (names.size() == 3) {
                catalog = names.get(0);
                schema = names.get(1);
                table = names.get(2);
            }
            if (sysInfoService == null) {
                sysInfoService = SysInfoServiceProvider.getRoot();
            }
            CommonId schemaId = sysInfoService.getSchemaIdByCache(schema);
            CommonId tableId = sysInfoService.getTableIdByCache(schemaId, table);

            EnumSet accessSet = EnumSet.noneOf(SqlAccessEnum.class);
            if (PrivilegeVerify.verify(user, host, schemaId, tableId,
                "select")) {
                accessSet.add(SqlAccessEnum.SELECT);
            }
            if (PrivilegeVerify.verify(user, host, schemaId, tableId,
                "insert")) {
                accessSet.add(SqlAccessEnum.INSERT);
            }
            if (PrivilegeVerify.verify(user, host, schemaId, tableId,
                "update")) {
                accessSet.add(SqlAccessEnum.UPDATE);
            }
            if (PrivilegeVerify.verify(user, host, schemaId, tableId,
                "delete")) {
                accessSet.add(SqlAccessEnum.DELETE);
            }
            return new SqlAccessType(accessSet);
        }
    }

    @Override
    public boolean supportsModality(SqlModality modality) {
        return relOptTable.supportsModality(modality);
    }

    @Override
    public boolean isTemporal() {
        return relOptTable.isTemporal();
    }

    @Override
    public <C> @Nullable C unwrap(Class<C> aClass) {
        return relOptTable.unwrap(aClass);
    }
}
