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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.exec.base.Job;
import io.dingodb.expr.json.runtime.Parser;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import java.util.Collections;
import java.util.List;
import java.util.Map;

final class DingoExplainSignature extends Meta.Signature {
    public static final Parser PARSER = Parser.JSON;

    @JsonProperty("physicalPlan")
    @Getter
    @Setter
    private String physicalPlan;

    @JsonProperty("logicalPlan")
    @Getter
    @Setter
    private String logicalPlan;

    @JsonProperty("job")
    @Getter
    @Setter
    private String job;

    public DingoExplainSignature(
        List<ColumnMetaData> columns,
        String sql,
        List<AvaticaParameter> parameters,
        Map<String, Object> internalParameters,
        Meta.CursorFactory cursorFactory,
        Meta.StatementType statementType,
        String physicalPlan,
        String logicalPlan,
        Job job
    ) {
        super(columns, sql, parameters, internalParameters, cursorFactory, statementType);
        this.physicalPlan = physicalPlan;
        this.logicalPlan = logicalPlan;
        this.job = job.toString();
    }

    @Override
    public String toString() {
        StringBuilder resultBuilder = new StringBuilder();
        String separate = String.join("", Collections.nCopies(100, "-"));

        // sql
        resultBuilder.append("SQL: \n" + this.physicalPlan + "\n");
        resultBuilder.append(separate + "\n");

        // logical plan
        resultBuilder.append("LOGICAL PLAN: \n");
        String[] operatorArray = this.logicalPlan.split("\n");
        for (int i = 0; i < operatorArray.length; i++) {
            resultBuilder.append(operatorArray[i].trim() + "\n");
        }
        resultBuilder.append(separate + "\n");

        // job
        resultBuilder.append("IMPLEMENTATION PLAN: \n" + this.job);
        return resultBuilder.toString();
    }
}
