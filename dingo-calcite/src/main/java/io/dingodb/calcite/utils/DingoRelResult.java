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

package io.dingodb.calcite.utils;

import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.linq4j.tree.BlockStatement;

public class DingoRelResult {
    public final BlockStatement block;

    /**
     * Describes the Java type returned by this relational expression, and the
     * mapping between it and the fields of the logical row type.
     */
    public final PhysType physType;
    public final JavaRowFormat format;

    public DingoRelResult(BlockStatement block, PhysType physType,
                  JavaRowFormat format) {
        this.block = block;
        this.physType = physType;
        this.format = format;
    }
}
