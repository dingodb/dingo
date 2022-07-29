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

package io.dingodb.common.operation;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ExecutiveResult implements Serializable {
    private static final long serialVersionUID = -5528925734091175454L;

    private List<Map<String, Value>> record;
    private boolean isSuccess;
    private String op;

    public ExecutiveResult(List<Map<String, Value>> record, boolean isSuccess) {
        this.record = record;
        this.isSuccess = isSuccess;
    }

    public List<Map<String, Value>> getRecord() {
        return record;
    }

    public boolean isSuccess() {
        return isSuccess;
    }
}
