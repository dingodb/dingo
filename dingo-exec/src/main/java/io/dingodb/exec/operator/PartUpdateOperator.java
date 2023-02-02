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

package io.dingodb.exec.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.expr.SqlExpr;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

@Slf4j
@JsonTypeName("update")
@JsonPropertyOrder({"table", "part", "schema", "keyMapping", "mapping", "updates", "output"})
public final class PartUpdateOperator extends PartModifyOperator {
    @JsonProperty("mapping")
    private final TupleMapping mapping;
    @JsonProperty("updates")
    private final List<SqlExpr> updates;

    @JsonCreator
    public PartUpdateOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") Object partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("mapping") TupleMapping mapping,
        @JsonProperty("updates") List<SqlExpr> updates
    ) {
        super(tableId, partId, schema, keyMapping);
        this.mapping = mapping;
        this.updates = updates;
    }

    @Override
    public void init() {
        super.init();
        updates.forEach(expr -> expr.compileIn(schema, getParasType()));
    }

    @Override
    public boolean pushTuple(Object[] tuple) {
        Object[] newTuple = Arrays.copyOf(tuple, tuple.length);
        boolean updated = false;
        int i = 0;
        try {
            for (i = 0; i < mapping.size(); ++i) {
                Object newValue = updates.get(i).eval(newTuple);
                int index = mapping.get(i);
                if ((tuple[index] == null && newValue != null)
                    || (tuple[index] != null && !tuple[index].equals(newValue))
                ) {
                    tuple[index] = newValue;
                    updated = true;
                }
            }
            if (updated) {
                part.upsert(Arrays.copyOf(tuple, schema.fieldCount()));
                count++;
            }
        } catch (Exception ex) {
            log.error("update operator with expr:{}, exception:{}",
                updates.get(i) == null ? "None" : updates.get(i).getExprString(),
                ex, ex);
            throw new RuntimeException("Update Operator catch Exception");
        }
        return true;
    }

    @Override
    public void setParas(Object[] paras) {
        super.setParas(paras);
        updates.forEach(e -> e.setParas(paras));
    }
}
