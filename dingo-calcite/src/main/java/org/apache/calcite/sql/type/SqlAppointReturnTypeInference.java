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

package org.apache.calcite.sql.type;

import com.google.common.collect.ImmutableList;
import io.dingodb.exec.fun.vector.VectorImageFun;
import io.dingodb.exec.fun.vector.VectorTextFun;
import org.apache.calcite.DingoSqlFloatType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.sql.type.ReturnTypes.explicit;

public class SqlAppointReturnTypeInference implements SqlReturnTypeInference  {
    private final SqlReturnTypeInference rule;
    private final ImmutableList<SqlTypeTransform> transforms;

    public static SqlAppointReturnTypeInference ret
        = new SqlAppointReturnTypeInference(ReturnTypes.ARG0, SqlTypeTransforms.TO_ARRAY);

    public static SqlReturnTypeInference FLOAT =
        explicit(SqlTypeName.FLOAT);

    /**
     * Creates a SqlTypeTransformCascade from a rule and an array of one or more.
     * transforms.
     *
     * @param rule rule
     * @param transforms transforms
     */
    public SqlAppointReturnTypeInference(SqlReturnTypeInference rule,
                                         SqlTypeTransform... transforms) {
        this.rule = rule;
        this.transforms = ImmutableList.copyOf(transforms);
    }

    @Override
    public @Nullable RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataType ret = rule.inferReturnType(opBinding);
        if (ret == null) {
            // inferReturnType may return null; transformType does not accept or
            // return null types
            return null;
        }
        if (opBinding.getOperator().getName().equalsIgnoreCase(VectorTextFun.NAME)
            || opBinding.getOperator().getName().equalsIgnoreCase(VectorImageFun.NAME)) {
            ret = new DingoSqlFloatType(SqlTypeName.FLOAT, 10);
        }
        for (SqlTypeTransform transform : transforms) {
            ret = transform.transformType(opBinding, ret);
        }
        return ret;
    }

}
