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

package io.dingodb.exec.expr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.dingodb.expr.runtime.CompileContext;
import lombok.ToString;

import java.util.Map;
import javax.annotation.Nonnull;

@ToString(of = {"parasNameType"})
public class SqlParasCompileContext implements CompileContext {
    @JsonValue
    private final Map<String, SqlParaCompileContext> parasNameType;

    @JsonCreator
    public SqlParasCompileContext(
        @Nonnull Map<String, SqlParaCompileContext> parasNameType
    ) {
        this.parasNameType = parasNameType;
        parasNameType.forEach((key, value) -> value.setId(key));
    }

    @Override
    public CompileContext getChild(@Nonnull Object index) {
        return parasNameType.get((String) index);
    }
}
