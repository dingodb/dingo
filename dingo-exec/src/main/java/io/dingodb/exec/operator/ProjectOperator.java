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
import io.dingodb.common.table.TupleSchema;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.util.ExprUtil;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TupleEvalContext;
import io.dingodb.expr.runtime.exception.FailGetEvaluator;

import java.util.List;
import javax.annotation.Nonnull;

@JsonTypeName("project")
@JsonPropertyOrder({"projects", "schema", "output"})
public final class ProjectOperator extends SoleOutOperator {
    @JsonProperty("projects")
    private final List<String> projects;
    @JsonProperty("schema")
    private final TupleSchema schema;

    private RtExpr[] exprs;

    @JsonCreator
    public ProjectOperator(
        @JsonProperty("projects") List<String> projects,
        @JsonProperty("schema") TupleSchema schema
    ) {
        super();
        this.projects = projects;
        this.schema = schema;
    }

    @Nonnull
    public static Object[] doProject(Object[] tuple, @Nonnull RtExpr[] projectExpr) {
        Object[] newTuple = new Object[projectExpr.length];
        for (int i = 0; i < newTuple.length; ++i) {
            try {
                newTuple[i] = projectExpr[i].eval(new TupleEvalContext(tuple));
            } catch (FailGetEvaluator e) {
                e.printStackTrace();
                newTuple[i] = null;
            }
        }
        return newTuple;
    }

    @Override
    public void init() {
        super.init();
        exprs = ExprUtil.compileExprList(projects, schema);
    }

    @Override
    public boolean push(int pin, Object[] tuple) {
        return output.push(doProject(tuple, exprs));
    }

    @Override
    public void fin(int pin, Fin fin) {
        output.fin(fin);
    }
}
