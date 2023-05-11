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

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BooleanMapper extends TypeMapper {

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? true : false;
        } else if (value instanceof Number) {
            return ((Number) value).intValue() != 0 ? true : false;
        } else if (value instanceof String) {
            /**
             * type 1. input "true" or "false"
             */
            if (value.toString().equalsIgnoreCase("true")) {
                return true;
            } else if (value.toString().equalsIgnoreCase("false")) {
                return false;
            }

            /**
             * type 2. input "1" or "0"
             */
            try {
                return Integer.parseInt(value.toString()) != 0 ? true : false;
            } catch (NumberFormatException e) {
                log.warn("Unable to parse value {} as boolean", value);
                return false;
            }
        } else {
            log.error("Unsupported boolean type: " + value.getClass().getName());
            throw new IllegalArgumentException("Unsupported boolean type: " + value.getClass().getName());
        }
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        return value;
    }
}
