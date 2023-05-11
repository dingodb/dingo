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

package io.dingodb.client.mappers;


import io.dingodb.sdk.common.DingoClientException;

import java.lang.reflect.Field;

public class EnumMapper extends TypeMapper {

    private final Class<? extends Enum<?>> clazz;
    private final String enumField;
    private final Field enumRequestedField;

    public EnumMapper(Class<? extends Enum<?>> clazz, String enumField) {
        this.clazz = clazz;
        this.enumField = enumField;
        if (!enumField.equals("")) {
            try {
                this.enumRequestedField = clazz.getDeclaredField(enumField);
                this.enumRequestedField.setAccessible(true);
            } catch (NoSuchFieldException e) {
                throw new DingoClientException("Cannot Map requested enum, issue with the requested enumField.");
            }
        } else {
            this.enumRequestedField = null;
        }
    }

    @Override
    public Object toDingoFormat(Object value) {
        if (!enumField.equals("")) {
            try {
                return enumRequestedField.get(value).toString();
            } catch (IllegalAccessException e) {
                throw new DingoClientException("Cannot Map requested enum, issue with the requested enumField.");
            }
        }
        return value.toString();
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }

        String stringValue = (String) value;
        Enum<?>[] constants = clazz.getEnumConstants();

        if (!enumField.equals("")) {
            try {
                for (Enum<?> thisEnum : constants) {
                    if (enumRequestedField.get(thisEnum).equals(stringValue)) {
                        return thisEnum;
                    }
                }
            } catch (IllegalAccessException e) {
                throw new DingoClientException("Cannot Map requested enum, issue with the requested enumField.");
            }
        } else {
            for (Enum<?> thisEnum : constants) {
                if (thisEnum.toString().equals(stringValue)) {
                    return thisEnum;
                }
            }
        }
        throw new DingoClientException(
            String.format("Enum value of \"%s\" not found in type %s", stringValue, clazz.toString())
        );
    }
}
