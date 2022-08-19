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
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.fin.Fin;

import java.util.List;
import java.util.Map;

@JsonTypeName("project")
@JsonPropertyOrder({"projects", "schema", "output"})
public final class ProjectOperator extends SoleOutOperator {
    @JsonProperty("projects")
    private final List<SqlExpr> projects;
    @JsonProperty("schema")
    private final DingoType schema;

    @JsonCreator
    public ProjectOperator(
        @JsonProperty("projects") List<SqlExpr> projects,
        @JsonProperty("schema") DingoType schema
    ) {
        super();
        this.projects = projects;
        this.schema = schema;
    }

    @Override
    public void init() {
        super.init();
        projects.forEach(expr -> expr.compileIn(schema, getParasCompileContext()));
    }

    @Override
    public synchronized boolean push(int pin, Object[] tuple) {
        Object[] newTuple = new Object[projects.size()];
        for (int i = 0; i < newTuple.length; ++i) {
            newTuple[i] = projects.get(i).eval(tuple);
        }
        return output.push(newTuple);
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        output.fin(fin);
    }

    @Override
    public void setParas(Map<String, Object> paras) {
        super.setParas(paras);
        projects.forEach(e -> e.setParas(paras));
    }
}
