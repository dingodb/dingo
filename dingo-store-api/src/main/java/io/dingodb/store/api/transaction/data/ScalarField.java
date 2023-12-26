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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class ScalarField {

    private Object data;

    public ScalarField(Boolean data) {
        this.data = data;
    }

    public ScalarField(Integer data) {
        this.data = data;
    }

    public ScalarField(Long data) {
        this.data = data;
    }

    public ScalarField(Float data) {
        this.data = data;
    }

    public ScalarField(Double data) {
        this.data = data;
    }

    public ScalarField(String data) {
        this.data = data;
    }

    public ScalarField(byte[] data) {
        this.data = data;
    }

}
