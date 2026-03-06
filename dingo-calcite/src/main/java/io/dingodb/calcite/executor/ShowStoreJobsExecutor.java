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

package io.dingodb.calcite.executor;

import io.dingodb.cluster.ClusterService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ShowStoreJobsExecutor extends QueryExecutor {

    private final ClusterService clusterService;
    private final Long jobId;
    private final Long archiveLimit;
    private final boolean includeArchive;
    private final Long archiveStartId;

    public ShowStoreJobsExecutor(Long jobId, Long archiveLimit,
                                 boolean includeArchive, Long archiveStartId) {
        this.clusterService = ClusterService.getDefault();
        this.jobId = jobId;
        this.archiveLimit = archiveLimit;
        this.includeArchive = includeArchive;
        this.archiveStartId = archiveStartId;
    }

    @Override
    Iterator<Object[]> getIterator() {
        return clusterService.getJobList(jobId, archiveLimit, includeArchive, archiveStartId).iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("id");
        columns.add("name");
        columns.add("nextStep");
        columns.add("taskSize");
        columns.add("createTime");
        columns.add("finishTime");
        return columns;
    }
}
