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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.sdk.service.entity.common.Schema;

import java.util.Iterator;
import java.util.List;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(Operator.class),
    @JsonSubTypes.Type(Comparator.class)
})
public interface Expr {

    io.dingodb.expr.runtime.expr.Expr toDingoExpr(
        List<String> attrNames, List<Schema> attrSchemas, List<Type> attrTypes
    );

    default void checkArgs(Iterator<Expr> args, OpType type) {
    if (!args.hasNext())
        throw new IllegalStateException(type + " operator arguments not support.");
    }

}
