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

package io.dingodb.exec.exception;

import io.dingodb.common.CommonId;
import io.dingodb.exec.fin.ErrorType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TaskFinException extends RuntimeException {
    private static final long serialVersionUID = 2818712157071556900L;

    @Getter
    private final CommonId jobId;

    @Getter
    private final ErrorType errorType;

    public TaskFinException(ErrorType errorType, String message, CommonId jobId) {
        super(message);
        this.errorType = errorType;
        this.jobId = jobId;
        log.warn("Job \"{}\" failed errorType is {} with error message: {}", jobId, errorType, message);
    }
}
