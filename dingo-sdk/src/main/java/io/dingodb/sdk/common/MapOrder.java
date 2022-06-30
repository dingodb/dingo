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

package io.dingodb.sdk.common;

public enum MapOrder {
    /**
     * Map is not ordered.  This is the default.
     */
    UNORDERED(0, 0x40),

    /**
     * Order map by key.
     */
    KEY_ORDERED(1, 0x80),

    /**
     * Order map by key, then value.
     */
    KEY_VALUE_ORDERED(3, 0xc0);

    public final int attributes;
    public final int flag;

    private MapOrder(int attributes, int flag) {
        this.attributes = attributes;
        this.flag = flag;
    }
}
