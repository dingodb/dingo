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

package io.dingodb.transaction.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.concurrent.CompletableFuture;

@Getter
@Builder
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class TableLock implements Comparable<TableLock> {

    @Builder.Default
    public final CommonId serverId = DingoConfiguration.serverId();
    public final CommonId tableId;
    public final long lockTs;
    public final long currentTs;

    public final LockType type;
    public final byte[] start;
    public final byte[] end;

    @EqualsAndHashCode.Exclude
    public transient final CompletableFuture<Boolean> lockFuture;
    @EqualsAndHashCode.Exclude
    public transient final CompletableFuture<Void> unlockFuture;

    @Override
    public int compareTo(TableLock o) {
        int compare = Long.compare(lockTs, o.lockTs);
        if (compare == 0) {
            return Long.compare(currentTs, o.currentTs);
        }
        return compare;
    }
}
