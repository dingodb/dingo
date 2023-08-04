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
import lombok.Getter;
import lombok.Setter;

public final class TableInfo {
    @Getter
    private final Exec create;
    @Getter
    private final Exec init;

    @Getter
    @Setter
    private String tableName;
    @Getter
    @Setter
    private transient boolean populated;

    public TableInfo(Exec create, Exec init) {
        this.create = create;
        this.init = init;
        tableName = null;
        populated = false;
    }
}
