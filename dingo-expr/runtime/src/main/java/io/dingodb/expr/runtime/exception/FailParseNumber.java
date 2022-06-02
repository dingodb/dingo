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

package io.dingodb.expr.runtime.exception;

public class FailParseNumber extends RuntimeException{
    private static final long serialVersionUID = -1049980909942382682L;


    private final String errorMessage;

    public FailParseNumber(String errorMessage) {
        super(
             errorMessage
        );
        this.errorMessage = errorMessage;
    }
}
