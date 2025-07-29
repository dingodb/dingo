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

package io.dingodb.exec.operator.params;

import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.expr.SqlExpr;

public class RepeatUnionParam extends FilterProjectSourceParam {
    public Job seed;
    public Job iteration;
    public boolean all;
    public int iterationLimit;

    public RepeatUnionParam(Job seedRelNode, Job iterationRelNode, boolean all, int iterationLimit) {
        this(null, null, null, 1, null, null, null, 2, seedRelNode, iterationRelNode, all, iterationLimit);
    }


    public RepeatUnionParam(CommonId tableId,
        CommonId partId, DingoType schema, int schemaVersion, SqlExpr filter,
        TupleMapping selection, TupleMapping keyMapping, int codecVersion, Job seedJob, Job iterationJob,
        boolean all, int iterationLimit
    ) {
        super(tableId, partId, schema, schemaVersion, filter, selection, keyMapping, codecVersion);
        this.seed = seedJob;
        this.iteration = iterationJob;
        this.all = all;
        this.iterationLimit = iterationLimit;
    }
}
