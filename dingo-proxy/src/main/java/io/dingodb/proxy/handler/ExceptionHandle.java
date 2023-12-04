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

package io.dingodb.proxy.handler;

import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.proxy.utils.Message;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import static io.dingodb.common.util.StackTraces.CURRENT_STACK;
import static io.dingodb.common.util.StackTraces.formatStackTrace;

@RestControllerAdvice
public class ExceptionHandle {

    @ExceptionHandler(value = Exception.class)
    @ResponseStatus
    @ResponseBody
    public Message handler(Exception e) {
        String stackTrace = formatStackTrace(e.getStackTrace(), CURRENT_STACK + 1, Integer.MAX_VALUE - CURRENT_STACK - 1);
        int code = -1;
        if (e instanceof DingoClientException) {
            code = ((DingoClientException) e).getErrorCode();
        }
        return new Message(code, e.getMessage(), stackTrace);
    }

}
