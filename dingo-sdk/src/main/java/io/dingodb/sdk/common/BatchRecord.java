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

package io.dingodb.sdk.common;

import io.dingodb.common.operation.Operation;

public class BatchRecord {

    public final Key key;

    public Record record;

    public final boolean hasWrite;

    public final Operation[] ops;

    public BatchRecord(Key key, Operation[] ops, boolean hasWrite) {
        this.key = key;
        this.ops = ops;
        this.hasWrite = hasWrite;
    }

    public BatchRecord(Key key, Record record, Operation[] ops, boolean hasWrite) {
        this.key = key;
        this.record = record;
        this.ops = ops;
        this.hasWrite = hasWrite;
    }
}
