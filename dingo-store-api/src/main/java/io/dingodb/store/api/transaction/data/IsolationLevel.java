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

package io.dingodb.store.api.transaction.data;

public enum IsolationLevel {
    InvalidIsolationLevel(0),  // this is just a placeholder, not a valid isolation level
    SnapshotIsolation(1),
    ReadCommitted(2);

    private final int code;

    IsolationLevel(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static IsolationLevel of(int code) {
        switch (code) {
            case 0: return InvalidIsolationLevel;
            case 1: return SnapshotIsolation;
            case 2: return ReadCommitted;
            default:
                throw new IllegalStateException("Unexpected value: " + code);
        }
    }
}
