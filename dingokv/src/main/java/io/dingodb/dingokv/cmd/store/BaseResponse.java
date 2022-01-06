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

package io.dingodb.dingokv.cmd.store;

import io.dingodb.dingokv.errors.Errors;
import io.dingodb.dingokv.metadata.RegionEpoch;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Getter
@Setter
@ToString
public class BaseResponse<T> implements Serializable {
    private static final long serialVersionUID = 8411573936817037697L;

    private Errors error = Errors.NONE;
    private String regionId;
    private RegionEpoch regionEpoch;
    private T value;

    public boolean isSuccess() {
        return error == Errors.NONE;
    }
}
