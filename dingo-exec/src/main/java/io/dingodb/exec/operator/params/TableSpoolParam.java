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

import io.dingodb.common.type.DingoType;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TableSpoolParam extends AbstractParams {
    public Collection list;
    public Collection tempCollection;
    @Setter
    @Getter
    private List<SqlExpr> projects;
    @Setter
    private DingoType schema;

    public TableSpoolParam(Collection list) {
        this.list = list;
        this.tempCollection = new ArrayList();
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        if (projects != null) {
            projects.forEach(expr -> expr.compileIn(schema, vertex.getParasType()));
        }
    }
}
