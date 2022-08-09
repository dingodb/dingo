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

import java.util.List;

public class LongListSchema implements DingoSchema {
    private int index;
    private boolean notNull;
    private List<Long> defaultValue;

    public LongListSchema(int index) {
        setIndex(index);
        setNotNull(false);
    }

    public LongListSchema(int index, Object defaultValue) {
        setIndex(index);
        setNotNull(true);
        setDefaultValue(defaultValue);
    }

    @Override
    public Type getType() {
        return Type.LONGLIST;
    }

    @Override
    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public void setLength(int length) {
        throw new UnsupportedOperationException("LongList Schema data length always be 0 (not sure)");
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void setMaxLength(int maxLength) {
        throw new UnsupportedOperationException("LongList Schema not support maxLength");
    }

    @Override
    public int getMaxLength() {
        return 0;
    }

    @Override
    public void setPrecision(int precision) {
        throw new UnsupportedOperationException("LongList Schema not support Precision");
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public void setScale(int scale) {
        throw new UnsupportedOperationException("LongList Schema not support Scale");
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public void setNotNull(boolean notNull) {
        this.notNull = notNull;
    }

    @Override
    public boolean isNotNull() {
        return notNull;
    }

    @Override
    public void setDefaultValue(Object defaultValue) throws ClassCastException {
        this.defaultValue = (List<Long>) defaultValue;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }
}
