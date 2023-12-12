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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.expr.SqlExpr;
import lombok.Getter;

@Getter
@JsonTypeName("infoSchemaScan")
@JsonPropertyOrder({"schema", "filter", "selection", "target"})
public class InfoSchemaScanParam extends FilterProjectSourceParam {
    @JsonProperty("schema")
    private final DingoType schema;

    @JsonProperty("filter")
    private final SqlExpr filter;

    @JsonProperty("selection")
    private final TupleMapping selection;

    @JsonProperty("target")
    private final String target;

    public InfoSchemaScanParam(DingoType schema,
                               SqlExpr filter,
                               TupleMapping selection,
                               String target) {
        super(null, null, schema, filter, selection, null);
        this.schema = schema;
        this.filter = filter;
        this.selection = selection;
        this.target = target;
    }
}
