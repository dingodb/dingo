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

package io.dingodb.client.model;

import io.dingodb.client.annotation.DingoRecord;
import lombok.Getter;

@DingoRecord(database = "test", table = "testSet")
@Getter
public class ChildClass {
    private int valueOfInt;
    private String valueOfStr;
    private float valueOfFloat;

    public ChildClass() {
    }

    public ChildClass(int valueOfInt, String valueOfStr, float valueOfFloat) {
        super();
        this.valueOfInt = valueOfInt;
        this.valueOfStr = valueOfStr;
        this.valueOfFloat = valueOfFloat;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        ChildClass other = (ChildClass) obj;
        return this.valueOfInt == other.valueOfInt
            && this.valueOfFloat == other.valueOfFloat
            && ((this.valueOfStr == null && other.valueOfStr == null)
            || (this.valueOfStr != null && this.valueOfStr.equals(other.valueOfStr)));
    }
}
