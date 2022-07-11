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

package io.dingodb.sdk.compute.str;

import io.dingodb.sdk.Cloneable;
import io.dingodb.sdk.common.Value;

import java.io.Serializable;

public class ComputeString implements Cloneable, Serializable {

    private String value;

    ComputeString(String str) {
        this.value = str;
    }

    public static ComputeString of(String value) {
        return new ComputeString(value == null ? "" : value);
    }

    public ComputeString append(ComputeString str) {
        value = value.concat(str.value().toString());
        return this;
    }

    public ComputeString replace(ComputeString str, ComputeString s) {
        value = value.replace(str.value, s.value);
        return this;
    }

    @Override
    public ComputeString fastClone() {
        return new ComputeString(value);
    }

    public Value value() {
        return Value.get(value);
    }

}
