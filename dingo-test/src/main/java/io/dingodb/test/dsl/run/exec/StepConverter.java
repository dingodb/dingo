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

package io.dingodb.test.dsl.run.exec;

import io.dingodb.test.dsl.builder.step.CustomStep;
import io.dingodb.test.dsl.builder.step.SqlFileNameStep;
import io.dingodb.test.dsl.builder.step.SqlFileStep;
import io.dingodb.test.dsl.builder.step.SqlStringStep;
import io.dingodb.test.dsl.builder.step.StepVisitor;
import io.dingodb.test.dsl.run.check.SqlCheckerConverter;
import io.dingodb.test.utils.ResourceFileUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class StepConverter implements StepVisitor<Exec> {
    private static final StepConverter INSTANCE = new StepConverter(
        null,
        null,
        SqlCheckerConverter.of(null, null)
    );

    private final Class<?> callerClass;
    private final String basePath;
    private final SqlCheckerConverter checkerConverter;

    public static StepConverter of(Class<?> callerClass, String basePath) {
        if (callerClass == null) {
            return INSTANCE;
        }
        return new StepConverter(callerClass, basePath, SqlCheckerConverter.of(callerClass, basePath));
    }

    @Override
    public Exec visit(@NonNull CustomStep step) {
        return step.getExec();
    }

    @Override
    public Exec visit(@NonNull SqlStringStep step) {
        return new ExecSql(
            step.getSqlString(),
            checkerConverter.safeVisit(step.getChecker())
        );
    }

    @Override
    public Exec visit(@NonNull SqlFileStep step) {
        return new ExecSql(
            ResourceFileUtils.readString(step.getSqlFile()),
            checkerConverter.safeVisit(step.getChecker())
        );
    }

    @Override
    public Exec visit(@NonNull SqlFileNameStep step) {
        return new ExecSql(
            ResourceFileUtils.readString(ResourceFileUtils.getResourceFile(basePath + "/" + step.getFileName(), callerClass)),
            checkerConverter.safeVisit(step.getChecker())
        );
    }
}
