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

package io.dingodb.server.executor.ddl;

import lombok.Builder;
import lombok.Data;

@Data
public class BackFillResult {
    int taskId;
    long addCount;
    long scanCount;
    byte[] nextKey;
    String error;

    public BackFillResult() {

    }

    @Builder
    public BackFillResult(int taskId, long addCount, long scanCount, byte[] nextKey, String error) {
        this.taskId = taskId;
        this.addCount = addCount;
        this.scanCount = scanCount;
        this.nextKey = nextKey;
        this.error = error;
    }

    public void addCount(int count) {
        this.addCount += count;
    }
}
