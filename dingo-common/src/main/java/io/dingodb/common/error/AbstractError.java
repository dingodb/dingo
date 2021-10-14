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

package io.dingodb.common.error;

/**
 * Abstract class implements DingoError interface to provide convenient {@link Object#toString()} override. This class
 * is provided for non enum {@link DingoError} implementations.
 */
public abstract class AbstractError implements DingoError {
    /**
     * Override to provide message for default string formation. For exuberant diagnostic message, use custom logging
     * function, since this function **does not contain stack trace** in result string for DingoException.
     *
     * @return informational string includes class name, error code, error info and possible error message.
     */
    @Override
    public String toString() {
        return ErrorUtil.toString(this);
    }
}
