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

package io.dingodb.calcite.rule;

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.traits.DingoConvention;
import io.dingodb.calcite.traits.DingoRelStreaming;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.stream.Collectors;

public class DingoTableModifyRule extends ConverterRule {
    public static final Config DEFAULT = Config.INSTANCE
        .withConversion(
            LogicalTableModify.class,
            Convention.NONE,
            DingoConvention.INSTANCE,
            "DingoTableModifyRule"
        )
        .withRuleFactory(DingoTableModifyRule::new);

    protected DingoTableModifyRule(Config config) {
        super(config);
    }

    private static void checkUpdateInPart(@NonNull LogicalTableModify rel) {
        Table td = rel.getTable().unwrap(DingoTable.class).getTable();
        List<String> updateList = rel.getUpdateColumnList();
        TupleMapping keyMapping = td.keyMapping();
        List<String> keys = keyMapping.stream()
            .mapToObj(td.getColumns()::get)
            .map(Column::getName).collect(Collectors.toList());
        if (updateList != null && updateList.stream().anyMatch(keys::contains)) {
            throw new IllegalStateException(
                "Update columns " + updateList + " contain primary columns and are not supported."
            );
        }
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        LogicalTableModify modify = (LogicalTableModify) rel;
        switch (modify.getOperation()) {
            case UPDATE:
                // Only support update in part.
                checkUpdateInPart(modify);
                break;
            case INSERT:
            case DELETE:
                break;
            default:
                throw new IllegalStateException(
                    "Operation \"" + modify.getOperation() + "\" is not supported."
                );
        }
        RelTraitSet traits = modify.getTraitSet()
            .replace(DingoConvention.INSTANCE)
            .replace(DingoRelStreaming.of(null, modify.getTable()));
        return new DingoTableModify(
            modify.getCluster(),
            traits,
            modify.getTable(),
            modify.getCatalogReader(),
            convert(modify.getInput(), traits),
            modify.getOperation(),
            modify.getUpdateColumnList(),
            modify.getSourceExpressionList(),
            modify.isFlattened()
        );
    }
}
