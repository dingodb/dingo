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
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.expr.RtExprWithType;
import io.dingodb.expr.runtime.TupleEvalContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

@Slf4j
@JsonTypeName("update")
@JsonPropertyOrder({"table", "part", "schema", "keyMapping", "mapping", "updates", "output"})
public final class PartUpdateOperator extends PartModifyOperator {
    @JsonProperty("mapping")
    private final TupleMapping mapping;
    @JsonProperty("updates")
    private final List<RtExprWithType> updates;

    @JsonCreator
    public PartUpdateOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") Object partId,
        @JsonProperty("schema") TupleSchema schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("mapping") TupleMapping mapping,
        @JsonProperty("updates") List<RtExprWithType> updates
    ) {
        super(tableId, partId, schema, keyMapping);
        this.mapping = mapping;
        this.updates = updates;
    }

    @Override
    public void init() {
        super.init();
        updates.forEach(expr -> expr.compileIn(schema));
    }

    @Override
    public synchronized boolean push(int pin, @Nonnull Object[] tuple) {
        TupleEvalContext etx = new TupleEvalContext(Arrays.copyOf(tuple, tuple.length));
        boolean update = false;
        for (int i = 0; i < mapping.size(); ++i) {
            Object newValue = updates.get(i).eval(etx);
            int index = mapping.get(i);
            if (!tuple[index].equals(newValue)) {
                tuple[index] = newValue;
                update = true;
            }
        }
        if (update) {
            part.upsert(tuple);
            count++;
        }
        return true;
    }
}
