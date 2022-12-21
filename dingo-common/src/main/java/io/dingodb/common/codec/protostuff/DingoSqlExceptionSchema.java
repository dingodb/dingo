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

import io.dingodb.common.exception.DingoSqlException;
import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;

public class DingoSqlExceptionSchema implements Schema<DingoSqlException> {
    public static final DingoSqlExceptionSchema INSTANCE = new DingoSqlExceptionSchema();
    private static final String[] fields = new String[]{"MSG", "CODE", "STATE"};

    private DingoSqlExceptionSchema() {
    }

    @Override
    public String getFieldName(int number) {
        try {
            return fields[number - 1];
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    @Override
    public int getFieldNumber(String name) {
        for (int i = 0; i < fields.length; ++i) {
            if (fields[i].equals(name)) {
                return i + 1;
            }
        }
        return 0;
    }

    @Override
    public boolean isInitialized(DingoSqlException message) {
        return message != null;
    }

    @Override
    public DingoSqlException newMessage() {
        return new DingoSqlException();
    }

    @Override
    public String messageName() {
        return typeClass().getSimpleName();
    }

    @Override
    public String messageFullName() {
        return typeClass().getName();
    }

    @Override
    public Class<? super DingoSqlException> typeClass() {
        return DingoSqlException.class;
    }

    @Override
    public void mergeFrom(@NonNull Input input, DingoSqlException message) throws IOException {
        while (true) {
            int number = input.readFieldNumber(this);
            switch (number) {
                case 0:
                    return;
                case 1:
                    message.setMessage(input.readString());
                    break;
                case 2:
                    message.setSqlCode(input.readInt32());
                    break;
                case 3:
                    message.setSqlState(input.readString());
                    break;
                default:
                    input.handleUnknownField(number, this);
            }
        }
    }

    @Override
    public void writeTo(@NonNull Output output, @NonNull DingoSqlException message) throws IOException {
        output.writeString(1, message.getMessage(), false);
        output.writeInt32(2, message.getSqlCode(), false);
        output.writeString(3, message.getSqlState(), false);
    }
}
