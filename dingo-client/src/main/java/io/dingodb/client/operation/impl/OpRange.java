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

package io.dingodb.client.operation.impl;

import io.dingodb.sdk.common.Range;
import lombok.Getter;
import lombok.experimental.Delegate;

@Getter
public class OpRange {

    @Delegate
    public final Range range;
    public final boolean withStart;
    public final boolean withEnd;

    public OpRange(byte[] startKey, byte[] endKey, boolean withStart, boolean withEnd) {
        this.range = new Range(startKey, endKey);
        this.withStart = withStart;
        this.withEnd = withEnd;
    }

    public OpRange(Range range, boolean withStart, boolean withEnd) {
        this.range = range;
        this.withStart = withStart;
        this.withEnd = withEnd;
    }
}
