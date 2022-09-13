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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;

public class DingoSqlTypeFactory extends JavaTypeFactoryImpl {
    public static DingoSqlTypeFactory INSTANCE = new DingoSqlTypeFactory();

    private DingoSqlTypeFactory() {
        super();
    }

    @Override
    public RelDataType createMultisetType(RelDataType type, long maxCardinality) {
        assert maxCardinality == -1;
        return new DingoMultisetType(type, false);
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        if (type instanceof DingoMultisetType) {
            DingoMultisetType dmt = (DingoMultisetType) type;
            RelDataType elementType = copyType(dmt.getComponentType());
            return new DingoMultisetType(elementType, nullable);
        }
        // This will copy any multiset type as `MultisetSqlType`.
        return super.createTypeWithNullability(type, nullable);
    }
}
