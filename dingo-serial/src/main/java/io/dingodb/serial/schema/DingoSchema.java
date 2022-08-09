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

package io.dingodb.serial.schema;

public interface DingoSchema {
    Type getType();

    void setIndex(int index);

    int getIndex();

    void setLength(int length);

    int getLength();

    void setMaxLength(int maxLength);

    int getMaxLength();

    void setPrecision(int precision);

    int getPrecision();

    void setScale(int scale);

    int getScale();

    void setNotNull(boolean notNull);

    boolean isNotNull();

    void setDefaultValue(Object defaultValue) throws ClassCastException;

    Object getDefaultValue();
}
