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

package io.dingodb.sdk.operation;

import io.dingodb.common.store.KeyValue;

import java.util.List;

public final class ResultForStore {
    private final boolean isSuccess;
    private final String  errorMessage;
    private final List<KeyValue> records;

    public ResultForStore(boolean isSuccess) {
        this(isSuccess, null, null);
    }

    public ResultForStore(boolean isSuccess, String errorMessage) {
        this(isSuccess, errorMessage, null);
    }

    public ResultForStore(boolean isSuccess, String errorMessage, List<KeyValue> records) {
        this.isSuccess = isSuccess;
        this.errorMessage = errorMessage;
        this.records = records;
    }

    public boolean getStatus() {
        return isSuccess;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public List<KeyValue> getRecords() {
        return records;
    }
}
