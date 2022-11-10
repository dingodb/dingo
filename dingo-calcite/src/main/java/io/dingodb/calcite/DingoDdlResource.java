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

package io.dingodb.calcite;

import org.apache.calcite.runtime.Resources;

import java.sql.SQLException;

public interface DingoDdlResource {
    DingoDdlResource INSTANCE = Resources.create(DingoDdlResource.class);

    @Resources.BaseMessage("Unsupported schema \"{0}\".")
    Resources.ExInst<SQLException> unsupportedSchema(String schemaName);
}
