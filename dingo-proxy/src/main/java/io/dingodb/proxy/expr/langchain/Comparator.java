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

package io.dingodb.proxy.expr.langchain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.type.BoolType;
import io.dingodb.expr.runtime.type.DoubleType;
import io.dingodb.expr.runtime.type.FloatType;
import io.dingodb.expr.runtime.type.IntType;
import io.dingodb.expr.runtime.type.LongType;
import io.dingodb.expr.runtime.type.StringType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.sdk.service.entity.common.Schema;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.util.List;

@Data
@NoArgsConstructor
@FieldNameConstants
@JsonTypeName("comparator")
public class Comparator implements Expr {
    @JsonProperty("comparator")
    private OpType comparator;
    @JsonProperty("attribute")
    private String attribute;
    @JsonProperty("value")
    private Object value;
    @JsonProperty("value_type")
    private Type valueType;

    public io.dingodb.expr.runtime.expr.Expr toDingoExpr(
        List<String> attrNames, List<Schema> attrSchemas, List<Type> attrTypes
    ) {
        if (this.attribute == null) {
            throw new IllegalArgumentException("Attribute null!");
        }
        int index = attrNames.indexOf(attribute);
        if (index == -1) {
            index = attrSchemas.size();
            attrNames.add(attribute);
            attrTypes.add(valueType);
            attrSchemas.add(Schema.builder().index(index).name(attribute).type(mapping(valueType)).isNullable(true).build());
        }
        switch (comparator) {
            case LT:
                return Exprs.LT.compile(Exprs.var(index, valueType), Exprs.val(value, valueType), ExprConfig.SIMPLE);
            case LE:
                return Exprs.LE.compile(Exprs.var(index, valueType), Exprs.val(value, valueType), ExprConfig.SIMPLE);
            case EQ:
                return Exprs.EQ.compile(Exprs.var(index, valueType), Exprs.val(value, valueType), ExprConfig.SIMPLE);
            case GT:
                return Exprs.GT.compile(Exprs.var(index, valueType), Exprs.val(value, valueType), ExprConfig.SIMPLE);
            case GE:
                return Exprs.GE.compile(Exprs.var(index, valueType), Exprs.val(value, valueType), ExprConfig.SIMPLE);
            case NE:
                return Exprs.NE.compile(Exprs.var(index, valueType), Exprs.val(value, valueType), ExprConfig.SIMPLE);
            default:
                throw new IllegalStateException("Unexpected value: " + comparator);
        }
    }

    private io.dingodb.sdk.service.entity.common.Type mapping(Type relType) {
        if (relType instanceof StringType) {
            return io.dingodb.sdk.service.entity.common.Type.STRING;
        }
        if (relType instanceof IntType) {
            return io.dingodb.sdk.service.entity.common.Type.INTEGER;
        }
        if (relType instanceof LongType) {
            return io.dingodb.sdk.service.entity.common.Type.LONG;
        }
        if (relType instanceof DoubleType) {
            return io.dingodb.sdk.service.entity.common.Type.DOUBLE;
        }
        if (relType instanceof FloatType) {
            return io.dingodb.sdk.service.entity.common.Type.FLOAT;
        }
        if (relType instanceof BoolType) {
            return io.dingodb.sdk.service.entity.common.Type.BOOL;
        }
        throw new IllegalStateException("Not support type: " + relType);
    }

}
