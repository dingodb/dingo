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

package io.dingodb.dingokv.metadata;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class StoreStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private long storeId;
    private long capacity;
    private long available;
    private int regionCount;
    private int leaderRegionCount;
    private int sendingSnapCount;
    private int receivingSnapCount;
    private int applyingSnapCount;
    private long startTime;
    private boolean busy;
    private long usedSize;
    private long bytesWritten;
    private long bytesRead;
    private long keysWritten;
    private long keysRead;
    private TimeInterval interval;
}
