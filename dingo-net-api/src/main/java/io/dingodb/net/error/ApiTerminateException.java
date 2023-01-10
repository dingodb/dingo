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

package io.dingodb.net.error;

/**
 * Thrown at the API definition side.
 * When the API execution throws this exception, no anything is returned to the caller, caller might block.
 */
public class ApiTerminateException extends RuntimeException {
    public ApiTerminateException(String message, Object...args) {
        super(String.format(message, args));
    }

    public ApiTerminateException(Throwable throwable, String message, Object...args) {
        super(String.format(message, args), throwable);
    }

}
