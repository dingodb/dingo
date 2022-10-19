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

import io.dingodb.common.type.DingoType;
import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class JobManagerImpl implements JobManager {
    public static final JobManagerImpl INSTANCE = new JobManagerImpl();

    private final Map<Id, Job> jobMap = new ConcurrentHashMap<>();

    private JobManagerImpl() {
    }

    @Override
    public @NonNull Job createJob(Id jobId, DingoType parasType) {
        Job job = new JobImpl(jobId, parasType);
        jobMap.put(jobId, job);
        return job;
    }

    @Override
    public Job getJob(Id jobId) {
        return jobMap.get(jobId);
    }

    @Override
    public Job removeJob(Id jobId) {
        return jobMap.remove(jobId);
    }
}
