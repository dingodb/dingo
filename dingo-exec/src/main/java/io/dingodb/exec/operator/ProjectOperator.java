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

import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.ProjectParam;

import java.util.List;

public final class ProjectOperator extends SoleOutOperator {
    public static final ProjectOperator INSTANCE = new ProjectOperator();

    private ProjectOperator() {
    }

    @Override
    public  boolean push(Context context, Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            ProjectParam param = vertex.getParam();
            List<SqlExpr> projects = param.getProjects();
            Object[] newTuple = new Object[projects.size()];
            for (int i = 0; i < newTuple.length; ++i) {
                newTuple[i] = projects.get(i).eval(tuple);
            }
            return vertex.getSoleEdge().transformToNext(context, newTuple);
        }
    }

    @Override
    public  void fin(int pin, Fin fin, Vertex vertex) {
        vertex.getSoleEdge().fin(fin);
    }

}
