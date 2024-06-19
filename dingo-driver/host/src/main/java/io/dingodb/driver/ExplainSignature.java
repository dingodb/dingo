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

package io.dingodb.driver;

import io.dingodb.calcite.visitor.DingoExplainVisitor;
import io.dingodb.common.mysql.Explain;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.dingodb.calcite.rel.DingoRel.dingo;

public class ExplainSignature extends Meta.Signature {
    RelNode relNode;

    public ExplainSignature(List<ColumnMetaData> columns,
                            String sql,
                            List<AvaticaParameter> parameters,
                            Map<String, Object> internalParameters,
                            Meta.CursorFactory cursorFactory,
                            Meta.StatementType statementType,
                            RelNode relNode) {
        super(columns, sql, parameters, internalParameters, cursorFactory, statementType);
        this.relNode = relNode;
    }

    public Iterator<Object[]> getIterator() {
        DingoExplainVisitor explainVisitor = new DingoExplainVisitor();
        Explain explain = dingo(relNode).accept(explainVisitor);
        if (explain == null) {
            List<Object[]> emptyList = new ArrayList<>();
            return emptyList.iterator();
        }
        List<Object[]> outputs = new ArrayList<>();
        Explain.loopOutput(explain, outputs, "");
        return outputs.iterator();
    }

}
