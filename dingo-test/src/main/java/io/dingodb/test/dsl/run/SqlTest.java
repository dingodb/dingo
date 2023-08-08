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

package io.dingodb.test.dsl.run;

import io.dingodb.test.dsl.run.exec.Exec;
import io.dingodb.test.dsl.run.exec.SqlExecContext;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public final class SqlTest {

    @Getter
    private final String name;
    private final List<? extends Exec> steps;
    private final boolean enabled;
    // The mapping from placeholder to table id in running context.
    private final Map<String, String> preparedTableMapping;
    // The table ids in running context that were modified in this test.
    private final List<String> modifiedTableIds;

    public SqlTest(
        @NonNull String name,
        List<? extends Exec> steps,
        boolean enabled,
        Map<String, String> preparedTableMapping,
        List<String> modifiedTableIds
    ) {
        this.name = name;
        this.steps = steps;
        this.enabled = enabled;
        this.preparedTableMapping = preparedTableMapping;
        this.modifiedTableIds = modifiedTableIds;
    }

    public void run(@NonNull SqlRunningContext context) throws Exception {
        assumeTrue(enabled);
        SqlExecContext execContext = context.prepareExecContext(preparedTableMapping);
        try {
            for (Exec exec : steps) {
                exec.run(execContext);
            }
        } finally {
            modifiedTableIds.forEach(context::setModified);
            execContext.cleanUp(preparedTableMapping);
        }
    }
}
