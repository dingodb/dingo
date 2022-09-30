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

package io.dingodb.calcite.utils;

import io.dingodb.common.type.TupleMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class RelDataTypeUtils {
    private RelDataTypeUtils() {
    }

    public static RelDataType mapType(
        @Nonnull RelDataTypeFactory typeFactory,
        @Nonnull RelDataType relDataType,
        @Nullable TupleMapping selection
    ) {
        if (selection == null) {
            return relDataType;
        }
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        final List<RelDataTypeField> fieldList = relDataType.getFieldList();
        selection.stream()
            .mapToObj(fieldList::get)
            .forEach(builder::add);
        return builder.build();
    }
}
