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

public abstract class TypeMapper {
    public abstract Object toDingoFormat(Object value);


    /**
     * Some types need to know if they're mapped to the correct class.
     * If they do, they can override this method to glean that information
     *
     * @param value value
     * @param isSubclassOfKnownType is subclass of known type
     * @param isUnknownType is unknown type
     * @return object
     */
    public Object toDingoFormat(Object value, boolean isUnknownType, boolean isSubclassOfKnownType) {
        return toDingoFormat(value);
    }

    public abstract Object fromDingoFormat(Object value);
}
