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

package io.dingodb.exec.transaction.base;

public enum TransactionType {
    OPTIMISTIC(0), PESSIMISTIC(1), NONE(-1);
    private final int code;

    TransactionType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static TransactionType of(int code) {
        switch (code) {
            case -1: return NONE;
            case 0: return OPTIMISTIC;
            case 1: return PESSIMISTIC;
            default:
                throw new IllegalStateException("Unexpected value: " + code);
        }
    }
}
