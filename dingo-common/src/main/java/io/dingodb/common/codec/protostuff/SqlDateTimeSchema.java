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

package io.dingodb.common.codec.protostuff;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;

public abstract class SqlDateTimeSchema<T extends java.util.Date> implements Schema<T> {
    protected static final String T_FILED = "T";
    protected static final int T_INDEX = 1;

    public String getFieldName(int number) {
        return (number == T_INDEX) ? T_FILED : null;
    }

    public int getFieldNumber(@NonNull String name) {
        return name.equals(T_FILED) ? T_INDEX : 0;
    }

    public boolean isInitialized(T message) {
        return message != null;
    }

    public String messageName() {
        return typeClass().getSimpleName();
    }

    public String messageFullName() {
        return typeClass().getName();
    }

    public void mergeFrom(@NonNull Input input, T message) throws IOException {
        while (true) {
            int number = input.readFieldNumber(this);
            switch (number) {
                case 0:
                    return;
                case 1:
                    message.setTime(input.readInt64());
                    break;
                default:
                    input.handleUnknownField(number, this);
            }
        }
    }

    public void writeTo(@NonNull Output output, @NonNull T message) throws IOException {
        output.writeInt64(1, message.getTime(), false);
    }
}
