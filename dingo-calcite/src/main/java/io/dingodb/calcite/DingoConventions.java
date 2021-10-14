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

import io.dingodb.calcite.rel.DingoRel;
import org.apache.calcite.plan.Convention;

public final class DingoConventions {
    public static Convention DINGO = new Convention.Impl("DINGO", DingoRel.class);
    public static Convention DISTRIBUTED = new Convention.Impl("DINGO_DISTRIBUTED", DingoRel.class);
    public static Convention PARTITIONED = new Convention.Impl("DINGO_PARTITIONED", DingoRel.class);
    public static Convention ROOT = new Convention.Impl("DINGO_ROOT", DingoRel.class);

    private DingoConventions() {
    }
}
