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

package io.dingodb.calcite.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class DingoMultisetType extends MultisetSqlType {
    private static final long serialVersionUID = -4326171357655711303L;

    public DingoMultisetType(RelDataType elementType, boolean isNullable) {
        super(elementType, isNullable);
    }

    @Override
    public RelDataTypeFamily getFamily() {
        SqlTypeFamily family = typeName.getFamily();
        return family != null ? family : this;
    }
}
