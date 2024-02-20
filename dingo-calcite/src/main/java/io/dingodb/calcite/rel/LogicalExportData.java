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

package io.dingodb.calcite.rel;

import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

import java.util.List;
import java.util.TimeZone;

public class LogicalExportData extends SingleRel {

    @Getter
    private final String outfile;

    @Getter
    private final byte[] terminated;

    @Getter
    private final String statementId;

    @Getter
    private final String enclosed;

    @Getter
    private final byte[] lineTerminated;

    @Getter
    private final byte[] escaped;

    @Getter
    private final String charset;

    @Getter
    private final byte[] lineStarting;

    @Getter
    private final TimeZone timeZone;

    public LogicalExportData(RelOptCluster cluster,
                             RelTraitSet traits,
                             RelNode input,
                             String outfile,
                             byte[] terminated,
                             String statementId,
                             String enclosed,
                             byte[] lineTerminated,
                             byte[] escaped,
                             String charset,
                             byte[] lineStarting,
                             TimeZone timeZone) {
        super(cluster, traits, input);
        this.outfile = outfile;
        this.terminated = terminated;
        this.statementId = statementId;
        this.enclosed = enclosed;
        this.lineTerminated = lineTerminated;
        this.escaped = escaped;
        this.charset = charset;
        this.lineStarting = lineStarting;
        this.timeZone = timeZone;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.itemIf("outfile", outfile, outfile != null);
        return pw;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalExportData(getCluster(), traitSet, sole(inputs),
            outfile, terminated, statementId, enclosed, lineTerminated, escaped, charset, lineStarting, timeZone);
    }
}
