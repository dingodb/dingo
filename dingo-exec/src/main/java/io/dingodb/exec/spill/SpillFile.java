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

package io.dingodb.exec.spill;

import lombok.Getter;

import java.nio.file.Path;

/**
 * Metadata for a single spill file created by {@link SpillFileManager}.
 */
@Getter
public final class SpillFile {

    private final String operatorId;
    private final int fileId;
    private final Path path;
    private long rowCount;

    SpillFile(String operatorId, int fileId, Path path) {
        this.operatorId = operatorId;
        this.fileId = fileId;
        this.path = path;
        this.rowCount = 0L;
    }

    void incrementRowCount(long delta) {
        this.rowCount += delta;
    }

    @Override
    public String toString() {
        return "SpillFile{"
            + "operatorId='" + operatorId + '\''
            + ", fileId=" + fileId
            + ", path=" + path
            + ", rowCount=" + rowCount
            + '}';
    }
}
