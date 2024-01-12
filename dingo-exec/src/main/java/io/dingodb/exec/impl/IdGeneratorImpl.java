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

package io.dingodb.exec.impl;

import io.dingodb.common.CommonId;
import io.dingodb.exec.base.IdGenerator;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class IdGeneratorImpl implements IdGenerator {
    private long currentValue = 0;

    private long jobSeqId = 0;

    public IdGeneratorImpl() {
    }

    public IdGeneratorImpl(long jobSeqId) {
        this.jobSeqId = jobSeqId;
    }


    @Override
    public @NonNull CommonId getOperatorId(long taskSeqId) {
        return new CommonId(io.dingodb.common.CommonId.CommonType.OP, taskSeqId, currentValue++);
    }

    @Override
    public @NonNull CommonId getOperatorId(CommonId taskId) {
        return new CommonId(io.dingodb.common.CommonId.CommonType.OP, taskId.seq, currentValue++);
    }

    @Override
    public @NonNull CommonId getTaskId(long jobSeqId) {
        return new CommonId(io.dingodb.common.CommonId.CommonType.TASK, jobSeqId, currentValue++);
    }

    @Override
    public @NonNull CommonId getTaskId() {
        return new CommonId(io.dingodb.common.CommonId.CommonType.TASK, jobSeqId, currentValue++);
    }

    @Override
    public @NonNull CommonId getJobId(long startTs, long jobSeqId) {
        return new CommonId(io.dingodb.common.CommonId.CommonType.JOB, startTs, jobSeqId);
    }

    @Override
    public @NonNull CommonId getJobId(long startTs) {
        return new CommonId(io.dingodb.common.CommonId.CommonType.JOB, startTs, jobSeqId);
    }
}
