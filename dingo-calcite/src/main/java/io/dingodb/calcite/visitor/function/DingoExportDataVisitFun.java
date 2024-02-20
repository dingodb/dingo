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

package io.dingodb.calcite.visitor.function;

import io.dingodb.calcite.rel.DingoExportData;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.ExportDataParam;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.function.Supplier;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.exec.utils.OperatorCodeUtils.EXPORT_DATA;

public final class DingoExportDataVisitFun {

    private DingoExportDataVisitFun() {
    }

    public static Collection<Vertex> visit(
        Job job,
        IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        @NonNull DingoExportData rel
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);

        return DingoBridge.bridge(idGenerator, inputs, new DingoExportDataVisitFun.OperatorSupplier(rel));

    }

    @AllArgsConstructor
    static class OperatorSupplier implements Supplier<Vertex> {

        final DingoExportData rel;

        @Override
        public Vertex get() {
            ExportDataParam exportDataParam = new ExportDataParam(
                rel.getOutfile(),
                rel.getTerminated(),
                rel.getStatementId(),
                rel.getEnclosed(),
                rel.getLineTerminated(),
                rel.getEscaped(),
                rel.getCharset(),
                rel.getLineStarting(),
                rel.getTimeZone()
            );

            return new Vertex(EXPORT_DATA, exportDataParam);
        }
    }

}
