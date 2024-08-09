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

package io.dingodb.common.ddl;

import io.dingodb.common.CommonId;
import lombok.Builder;
import lombok.Data;

@Data
public class ReorgBackFillTask {
    private CommonId tableId;
    private CommonId indexId;
    private long jobId;
    private String id;
    private byte[] start;
    private byte[] end;
    private boolean withStart;
    private boolean withEnd;
    private long startTs;
    private CommonId regionId;

    @Builder
    public ReorgBackFillTask(
        CommonId tableId,
        CommonId indexId,
        long jobId,
        String id,
        byte[] start,
        byte[] end,
        long startTs,
        boolean withStart,
        boolean withEnd,
        CommonId regionId
    ) {
        this.tableId = tableId;
        this.indexId = indexId;
        this.jobId = jobId;
        this.id = id;
        this.start = start;
        this.end = end;
        this.startTs = startTs;
        this.withStart = withStart;
        this.withEnd = withEnd;
        this.regionId = regionId;
    }
}
