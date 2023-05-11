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

package io.dingodb.client.utils;


import io.dingodb.sdk.common.DingoClientException;

public class NotAnnotatedClass extends DingoClientException {

    private static final long serialVersionUID = -4781097961894057707L;
    public static final int REASON_CODE = -109;

    public NotAnnotatedClass(String description) {
        super(REASON_CODE, description);
    }
}
